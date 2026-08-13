import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from motor.motor_asyncio import AsyncIOMotorClient
from typing import List

app = FastAPI(title="Real-Time Event Detector API (Academic Edition)")

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
    """Watch MongoDB for new events using Change Streams."""
    print(f"[Backend] Watching MongoDB collection '{collection.name}' for new events...")
    try:
        async with collection.watch([{"$match": {"operationType": "insert"}}]) as stream:
            async for change in stream:
                event = change["fullDocument"]
                if "_id" in event:
                    event["_id"] = str(event["_id"])
                
                await manager.broadcast(json.dumps(event))
    except Exception as e:
        print(f"[Backend] Change Stream Error on {collection.name}: {e}")

@app.on_event("startup")
async def startup_event():
    # Start the change stream listeners in the background
    asyncio.create_task(watch_mongo_changes(processed_collection))
    asyncio.create_task(watch_mongo_changes(meta_collection))

@app.on_event("shutdown")
async def shutdown_event():
    client.close()

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
