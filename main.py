import asyncio
import re
import json
import os
import uuid
import zipfile
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse
from curl_cffi.requests import AsyncSession

# Initialize FastAPI
app = FastAPI(title="Hitomi Scraper API")

# Setup Temporary directory for ephemeral disk writing (crucial for Render limits)
TEMP_DIR = Path("/tmp/hitomi_downloads")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# Regex to extract Gallery ID from URL
# Matches: https://hitomi.la/imageset/english-3441601.html#1
ID_REGEX = re.compile(r'-(\d+)\.html')

# --- Hitomi.la Routing Logic Translation ---
def get_subdomain(hash_str: str) -> str:
    """Approximates hitomi's 'gg.b' subdomain routing logic based on image hash."""
    g = int(hash_str[-1] + hash_str[-3:-1], 16)
    if g < 0x30:
        return 'a'
    elif g < 0x60:
        return 'b'
    else:
        return 'c'

def construct_image_url(image_data: dict) -> str:
    """Constructs the raw image URL from gallery info hash using the new CDN."""
    img_hash = image_data.get('hash')
    has_webp = image_data.get('haswebp', 0)
    
    # Prefer webp for faster downloads, fallback to original extension
    ext = 'webp' if has_webp else image_data.get('name', '').split('.')[-1]
    folder = 'webp' if has_webp else 'images'
    
    subdomain = get_subdomain(img_hash)
    # Updated: Hitomi moved image assets to gold-usergeneratedcontent.net
    hash_part = img_hash[-1] + '/' + img_hash[-3:-1]
    return f"https://{subdomain}a.gold-usergeneratedcontent.net/{folder}/{hash_part}/{img_hash}.{ext}"

async def fetch_gallery_metadata(gallery_id: str, session: AsyncSession):
    """Fetches the raw gallery JSON metadata using the updated CDN domain."""
    # Updated: ltn.hitomi.la is dead, replaced by ltn.gold-usergeneratedcontent.net
    url = f"https://ltn.gold-usergeneratedcontent.net/galleries/{gallery_id}.js"
    response = await session.get(url)
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch metadata: HTTP {response.status_code}")
    
    text = response.text.replace('var galleryinfo = ', '').strip()
    if text.endswith(';'):
        text = text[:-1]
    
    return json.loads(text)

async def download_image_with_retry(session: AsyncSession, url: str, index: int, ws: WebSocket, max_retries: int = 3) -> bytes:
    """Downloads an image with exponential backoff and retries."""
    for attempt in range(max_retries):
        try:
            # We need standard Referer headers to bypass hotlink protection
            headers = {"Referer": "https://hitomi.la/"}
            resp = await session.get(url, headers=headers, timeout=15)
            
            if resp.status_code == 200:
                return resp.content
            elif resp.status_code in [403, 503]:
                await ws.send_json({"type": "log", "msg": f"⚠️ Attempt {attempt+1}: Access denied for image {index}. Retrying..."})
            else:
                await ws.send_json({"type": "log", "msg": f"⚠️ Error {resp.status_code} for image {index}. Retrying..."})
                
        except Exception as e:
            await ws.send_json({"type": "log", "msg": f"⚠️ Attempt {attempt+1} failed for image {index}: {str(e)}"})
            
        await asyncio.sleep(2 ** (attempt + 1))
        
    await ws.send_json({"type": "log", "msg": f"❌ Failed to download image {index} after {max_retries} attempts."})
    return b""

@app.get("/")
async def serve_frontend():
    """Serves the single-file HTML frontend."""
    current_dir = Path(__file__).parent
    return HTMLResponse((current_dir / "index.html").read_text())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            url = data.get("url")
            
            match = ID_REGEX.search(url)
            if not match:
                await websocket.send_json({"type": "error", "msg": "Invalid Hitomi URL format."})
                continue
                
            gallery_id = match.group(1)
            
            async with AsyncSession(impersonate="chrome110") as session:
                try:
                    await websocket.send_json({"type": "log", "msg": f"Fetching metadata for gallery {gallery_id}..."})
                    metadata = await fetch_gallery_metadata(gallery_id, session)
                    images = metadata.get('files', [])
                    total_images = len(images)
                    
                    if action == "estimate":
                        start = max(1, int(data.get("start", 1)))
                        end = min(total_images, int(data.get("end", total_images)))
                        count = end - start + 1
                        
                        est_size = sum(300 if img.get('haswebp') else 500 for img in images[start-1:end])
                        await websocket.send_json({
                            "type": "estimate",
                            "msg": f"Gallery found! Selected {count} images. Estimated ZIP size: {est_size / 1024:.2f} MB",
                            "total": total_images
                        })
                    
                    elif action == "download":
                        start = max(1, int(data.get("start", 1)))
                        end = min(total_images, int(data.get("end", total_images)))
                        
                        await websocket.send_json({"type": "log", "msg": f"Preparing download for images {start} to {end}..."})
                        
                        target_images = images[start-1:end]
                        download_urls = [construct_image_url(img) for img in target_images]
                        
                        file_id = str(uuid.uuid4())
                        zip_path = TEMP_DIR / f"{gallery_id}_{file_id}.zip"
                        
                        semaphore = asyncio.Semaphore(4)
                        downloaded_count = 0
                        
                        async def bounded_download(url, idx, img_data):
                            nonlocal downloaded_count
                            async with semaphore:
                                await websocket.send_json({"type": "log", "msg": f"Downloading image {idx}..."})
                                content = await download_image_with_retry(session, url, idx, websocket)
                                if content:
                                    ext = 'webp' if img_data.get('haswebp') else img_data.get('name').split('.')[-1]
                                    filename = f"{idx:04d}.{ext}"
                                    
                                    with zipfile.ZipFile(zip_path, 'a', compression=zipfile.ZIP_STORED) as zipf:
                                        zipf.writestr(filename, content)
                                        
                                    downloaded_count += 1
                                    await websocket.send_json({
                                        "type": "progress",
                                        "progress": (downloaded_count / len(target_images)) * 100,
                                        "msg": f"Saved image {idx}."
                                    })
                        
                        tasks = [
                            bounded_download(url, start + i, target_images[i])
                            for i, url in enumerate(download_urls)
                        ]
                        await asyncio.gather(*tasks)
                        
                        await websocket.send_json({
                            "type": "done",
                            "msg": "✅ Packaging complete!",
                            "download_url": f"/api/download/{zip_path.name}"
                        })

                except Exception as e:
                    await websocket.send_json({"type": "error", "msg": f"Process failed: {str(e)}"})

    except WebSocketDisconnect:
        print("Client disconnected")

def remove_file(path: Path):
    try:
        if path.exists():
            path.unlink()
    except Exception as e:
        print(f"Failed to delete {path}: {e}")

@app.get("/api/download/{filename}")
async def download_zip(filename: str, background_tasks: BackgroundTasks):
    file_path = TEMP_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File expired or not found.")
    
    background_tasks.add_task(remove_file, file_path)
    return FileResponse(
        path=file_path, 
        filename=filename, 
        media_type='application/zip'
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
