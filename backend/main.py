import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from motor.motor_asyncio import AsyncIOMotorClient
from typing import List

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Real-Time Event Detector API (Academic Edition)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# MongoDB connection
MONGO_URL = "mongodb://localhost:27017"
client = AsyncIOMotorClient(MONGO_URL)
db = client.event_detector
processed_collection = db.processed_events
meta_collection = db.meta_events

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections.copy():
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()

async def watch_mongo_changes(collection):
    """Watch MongoDB for new events using Change Streams with a Polling fallback."""
    print(f"[Backend] Attempting to watch MongoDB collection '{collection.name}' using Change Streams...")
    try:
        async with collection.watch([{"$match": {"operationType": "insert"}}]) as stream:
            async for change in stream:
                event = change["fullDocument"]
                if "_id" in event:
                    event["_id"] = str(event["_id"])
                print(f"[Backend] Broadcasting event {event.get('title')} to {len(manager.active_connections)} clients")
                await manager.broadcast(json.dumps(event))
    except Exception as e:
        print(f"[Backend] Change Stream Error on {collection.name}: {e}")
        print(f"[Backend] Falling back to polling mechanism for '{collection.name}'...")
        # Polling fallback
        last_seen_id = None
        while True:
            try:
                # Get the most recent document to initialize if needed
                if last_seen_id is None:
                    latest = await collection.find_one(sort=[("_id", -1)])
                    if latest:
                        last_seen_id = latest["_id"]
                else:
                    # Find documents newer than last_seen_id
                    cursor = collection.find({"_id": {"$gt": last_seen_id}}).sort("_id", 1)
                    async for event in cursor:
                        last_seen_id = event["_id"]
                        event["_id"] = str(event["_id"])
                        await manager.broadcast(json.dumps(event))
            except Exception as poll_e:
                print(f"[Backend] Polling Error on {collection.name}: {poll_e}")
            await asyncio.sleep(1)

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start the change stream listeners in the background
    task1 = asyncio.create_task(watch_mongo_changes(processed_collection))
    task2 = asyncio.create_task(watch_mongo_changes(meta_collection))
    yield
    # Shutdown: Clean up tasks and db client
    task1.cancel()
    task2.cancel()
    client.close()

app = FastAPI(title="Real-Time Event Detector API (Academic Edition)", lifespan=lifespan)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/api/events")
async def get_recent_events(limit: int = 50):
    cursor = processed_collection.find().sort("ingested_at", -1).limit(limit)
    events = await cursor.to_list(length=limit)
    for event in events:
        event["_id"] = str(event["_id"])
    return events
