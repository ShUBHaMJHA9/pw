from telethon import TelegramClient
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

api_id = int(os.getenv("TELEGRAM_API_ID", 11468953))
api_hash = os.getenv("TELEGRAM_API_HASH", "99f7513ef4889752f6278af3286a929c")

# User session, not bot
client = TelegramClient("session", api_id, api_hash)


@app.on_event("startup")
async def startup():
    # Start the client (first time will ask phone & code)
    await client.start()
    print("Telegram client started!")


@app.get("/stream/{msg_id}")
async def stream(msg_id: int):
    # Fetch message from channel
    msg = await client.get_messages("@pwbacku", ids=msg_id)
    media = msg.video or msg.document

    if not media:
        return {"error": "No media found"}

    # Stream in chunks
    async def generator():
        async for chunk in client.iter_download(media, chunk_size=1024 * 1024):
            yield chunk

    headers = {}
    if getattr(media, "size", None):
        headers["Content-Length"] = str(media.size)

    return StreamingResponse(generator(), media_type="video/mp4", headers=headers)
