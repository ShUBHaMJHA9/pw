from telethon import TelegramClient
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
import os
from dotenv import load_dotenv
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputDocumentFileLocation

load_dotenv()

app = FastAPI()

api_id = int(os.getenv("TELEGRAM_API_ID"))
api_hash = os.getenv("TELEGRAM_API_HASH")

client = TelegramClient("session", api_id, api_hash)

CHUNK_SIZE = 512 * 1024  # 512KB


@app.on_event("startup")
async def startup():
    await client.start()
    print("Telegram client started!")


@app.get("/stream/{msg_id}")
async def stream(request: Request, msg_id: int):

    msg = await client.get_messages(-1003382065361, ids=msg_id)

    if not msg:
        raise HTTPException(404, "Message not found")

    media = msg.video or msg.document

    if not media:
        raise HTTPException(404, "No media found")

    file_size = media.size

    # ---- RANGE PARSING ----
    range_header = request.headers.get("range")
    start = 0
    end = file_size - 1

    if range_header:
        range_value = range_header.replace("bytes=", "")
        start_str, end_str = range_value.split("-")
        start = int(start_str) if start_str else 0
        end = int(end_str) if end_str else file_size - 1

    # ---- TELEGRAM LOCATION ----
    location = InputDocumentFileLocation(
        id=media.id,
        access_hash=media.access_hash,
        file_reference=media.file_reference,
        thumb_size=""
    )

    async def file_iterator(start: int, end: int):
        remaining = end - start + 1

        async for chunk in client.iter_download(
            media,
            offset=start,
            request_size=512 * 1024
        ):
            if remaining <= 0:
                break

            if len(chunk) > remaining:
                chunk = chunk[:remaining]

            yield chunk
            remaining -= len(chunk)

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(end - start + 1),
        "Content-Type": "video/mp4"
    }

    return StreamingResponse(
        file_iterator(start, end),
        status_code=206 if range_header else 200,
        headers=headers,
        media_type="video/mp4"
    )
