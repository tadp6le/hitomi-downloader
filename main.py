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

# Setup Temporary directory for ephemeral disk writing
TEMP_DIR = Path("/tmp/hitomi_downloads")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# Regex to extract Gallery ID from URL
ID_REGEX = re.compile(r'-(\d+)\.html')

# Fetch Proxy URL from Render Environment Variables
# Format expected: http://username:password@proxy-domain:port
PROXY_URL = os.environ.get("PROXY_URL", None)
PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

async def fetch_gallery_metadata(gallery_id: str, session: AsyncSession):
    """Fetches the raw gallery JSON metadata trying multiple known CDNs."""
    domains_to_try = [
        f"https://ltn.gold-usergeneratedcontent.net/galleries/{gallery_id}.js",
        f"https://ltn.hitomi.la/galleries/{gallery_id}.js"
    ]
    
    last_error = ""
    for url in domains_to_try:
        try:
            response = await session.get(url, timeout=15)
            if response.status_code == 200:
                text = response.text.replace('var galleryinfo = ', '').strip()
                if text.endswith(';'):
                    text = text[:-1]
                return json.loads(text)
            else:
                last_error = f"HTTP {response.status_code}"
        except Exception as e:
            last_error = str(e)
            continue
            
    raise Exception(f"Failed to fetch metadata. Last error: {last_error}")

async def download_image_with_retry(session: AsyncSession, img_data: dict, index: int, ws: WebSocket) -> bytes:
    """Brute-forces the image download with strict browser headers via proxy."""
    img_hash = img_data.get('hash')
    has_webp = img_data.get('haswebp', 0)
    
    ext = 'webp' if has_webp else img_data.get('name', '').split('.')[-1]
    folder = 'webp' if has_webp else 'images'
    hash_part = f"{img_hash[-1]}/{img_hash[-3:-1]}"
    
    subdomains = ['a', 'b', 'c', 'aa', 'ba', 'ca', 'da']
    base_domains = ['hitomi.la', 'gold-usergeneratedcontent.net']
    
    headers = {
        "Referer": "https://hitomi.la/",
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        "Sec-Fetch-Dest": "image",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "cross-site",
    }
    
    for attempt in range(3):
        last_error = "Unknown"
        
        for domain in base_domains:
            for sub in subdomains:
                url = f"https://{sub}.{domain}/{folder}/{hash_part}/{img_hash}.{ext}"
                try:
                    resp = await session.get(url, headers=headers, timeout=15)
                    
                    if resp.status_code == 200:
                        return resp.content
                    else:
                        last_error = f"HTTP {resp.status_code}"
                        
                except Exception as e:
                    last_error = str(e)
                    continue
                    
        await ws.send_json({"type": "log", "msg": f"⚠️ Attempt {attempt+1} failed for image {index} | Reason: {last_error}"})
        await asyncio.sleep(2 ** attempt)
        
    await ws.send_json({"type": "log", "msg": f"❌ Exhausted all routes for image {index}."})
    return b""

@app.get("/")
async def serve_frontend():
    current_dir = Path(__file__).parent
    return HTMLResponse((current_dir / "index.html").read_text())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    if PROXIES:
        await websocket.send_json({"type": "log", "msg": "🛡️ Proxy routing is ENABLED."})
    else:
        await websocket.send_json({"type": "log", "msg": "⚠️ No proxy detected. Using Render default IP (may be blocked)."})
        
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
            
            # Pass the proxy configuration to the session
            async with AsyncSession(impersonate="chrome110", proxies=PROXIES) as session:
                try:
                    if action in ["estimate", "download"]:
                        await websocket.send_json({"type": "log", "msg": f"Fetching metadata for gallery {gallery_id}..."})
                        metadata = await fetch_gallery_metadata(gallery_id, session)
                        images = metadata.get('files', [])
                        total_images = len(images)
                        
                        start = max(1, int(data.get("start", 1)))
                        end = min(total_images, int(data.get("end", total_images)))
                        count = end - start + 1
                    
                    if action == "estimate":
                        est_size = sum(300 if img.get('haswebp') else 500 for img in images[start-1:end])
                        await websocket.send_json({
                            "type": "estimate",
                            "msg": f"Gallery found! Selected {count} images. Estimated ZIP size: {est_size / 1024:.2f} MB",
                            "total": total_images
                        })
                    
                    elif action == "download":
                        await websocket.send_json({"type": "log", "msg": f"Preparing download for images {start} to {end}..."})
                        
                        target_images = images[start-1:end]
                        file_id = str(uuid.uuid4())
                        zip_path = TEMP_DIR / f"{gallery_id}_{file_id}.zip"
                        
                        semaphore = asyncio.Semaphore(4)
                        downloaded_count = 0
                        
                        async def bounded_download(idx, img_data):
                            nonlocal downloaded_count
                            async with semaphore:
                                await websocket.send_json({"type": "log", "msg": f"Locating and downloading image {idx}..."})
                                content = await download_image_with_retry(session, img_data, idx, websocket)
                                
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
                            bounded_download(start + i, target_images[i])
                            for i in range(len(target_images))
                        ]
                        await asyncio.gather(*tasks)
                        
                        if downloaded_count == 0:
                            await websocket.send_json({"type": "error", "msg": "Failed to download any images. Your proxy might be blocked, or routing failed."})
                        else:
                            await websocket.send_json({
                                "type": "done",
                                "msg": f"✅ Packaging complete! Secured {downloaded_count} images.",
                                "download_url": f"/api/download/{zip_path.name}"
                            })

                except Exception as e:
                    await websocket.send_json({"type": "error", "msg": f"Process failed: {str(e)}"})

    except WebSocketDisconnect:
        pass

def remove_file(path: Path):
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass

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
