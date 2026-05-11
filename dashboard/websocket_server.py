"""
WebSocket Server for Real-Time Dashboard Updates
Streams new events to connected dashboard clients
"""

import asyncio
import websockets
import json
from pymongo import MongoClient
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import MONGO_URI, MONGO_DB


# Global set to track connected clients
connected_clients = set()


async def event_stream(websocket, path):
    """
    Stream new events to connected clients.
    Each client gets its own connection and cursor.
    """
    client_id = id(websocket)
    print(f"📱 Client {client_id} connected from {websocket.remote_address}")

    # Register client
    connected_clients.add(websocket)

    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db["processed_events"]

        # Start from current time
        last_timestamp = datetime.utcnow()

        await websocket.send(json.dumps({
            "type": "connection",
            "message": "Connected to event stream",
            "timestamp": datetime.utcnow().isoformat()
        }))

        # Continuously check for new events
        while True:
            try:
                # Query for new events since last check
                query = {"ingested_at": {"$gt": last_timestamp}}

                new_events = list(collection
                                   .find(query)
                                   .sort("ingested_at", 1)
                                   .limit(50))  # Batch size

                if new_events:
                    # Update last timestamp
                    if new_events:
                        last_timestamp = max(
                            event.get("ingested_at", last_timestamp)
                            for event in new_events
                        )

                    # Send each event as a separate message
                    for event in new_events:
                        # Convert MongoDB ObjectId to string
                        event['_id'] = str(event['_id'])

                        # Create event message
                        message = {
                            "type": "event",
                            "data": event,
                            "timestamp": datetime.utcnow().isoformat()
                        }

                        await websocket.send(json.dumps(message, default=str))

                    # Send batch summary
                    summary = {
                        "type": "batch_summary",
                        "count": len(new_events),
                        "timestamp": datetime.utcnow().isoformat()
                    }

                    await websocket.send(json.dumps(summary, default=str))

                    print(f"📤 Sent {len(new_events)} events to client {client_id}")

                # Wait before next check
                await asyncio.sleep(2)  # Check every 2 seconds

            except Exception as e:
                print(f"❌ Error processing events for client {client_id}: {e}")
                error_msg = {
                    "type": "error",
                    "message": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
                await websocket.send(json.dumps(error_msg))
                await asyncio.sleep(5)  # Wait longer on error

    except websockets.exceptions.ConnectionClosed:
        print(f"📱 Client {client_id} disconnected")

    except Exception as e:
        print(f"❌ Error with client {client_id}: {e}")

    finally:
        # Unregister client
        connected_clients.discard(websocket)
        print(f"📱 Client {client_id} removed from connected clients")


async def broadcast_message(message: dict):
    """Broadcast a message to all connected clients"""
    if connected_clients:
        message_str = json.dumps(message, default=str)
        disconnected = set()

        for client in connected_clients:
            try:
                await client.send(message_str)
            except Exception as e:
                print(f"❌ Error broadcasting to client: {e}")
                disconnected.add(client)

        # Remove disconnected clients
        connected_clients -= disconnected


async def stats_server():
    """Background task to broadcast periodic statistics"""
    while True:
        try:
            client = MongoClient(MONGO_URI)
            db = client[MONGO_DB]
            processed_collection = db["processed_events"]

            # Get current statistics
            total_events = processed_collection.count_documents({})

            # Count by source type
            wiki_count = processed_collection.count_documents({"source_type": "wikipedia"})
            news_count = processed_collection.count_documents({"source_type": "news"})
            gdacs_count = processed_collection.count_documents({"source_type": "gdacs"})
            financial_count = processed_collection.count_documents({"source_type": "financial"})

            # Recent activity (last 5 minutes)
            from datetime import timedelta
            recent_cutoff = datetime.utcnow() - timedelta(minutes=5)
            recent_events = processed_collection.count_documents({
                "ingested_at": {"$gte": recent_cutoff}
            })

            stats_message = {
                "type": "stats",
                "data": {
                    "total_events": total_events,
                    "wikipedia": wiki_count,
                    "news": news_count,
                    "gdacs": gdacs_count,
                    "financial": financial_count,
                    "recent_events_5min": recent_events,
                    "connected_clients": len(connected_clients)
                },
                "timestamp": datetime.utcnow().isoformat()
            }

            await broadcast_message(stats_message)
            print(f"📊 Broadcast statistics to {len(connected_clients)} clients")

        except Exception as e:
            print(f"❌ Error broadcasting stats: {e}")

        await asyncio.sleep(30)  # Broadcast every 30 seconds


async def main():
    """Start the WebSocket server"""
    host = "localhost"
    port = 8765

    print(f"🚀 Starting WebSocket server on {host}:{port}")

    # Start the event stream server
    server = await websockets.serve(event_stream, host, port)

    # Start the stats broadcast task
    stats_task = asyncio.create_task(stats_server())

    print(f"✅ WebSocket server ready")
    print(f"📡 Connect to: ws://{host}:{port}")
    print(f"📊 Broadcasting statistics every 30 seconds")

    try:
        # Keep server running
        await asyncio.Future()
    except KeyboardInterrupt:
        print("\n🛑 Server shutting down...")
        stats_task.cancel()
        server.close()
        await server.wait_closed()
        print("✅ Server stopped cleanly")


if __name__ == "__main__":
    print("=" * 60)
    print("Real-Time Event Detection WebSocket Server")
    print("=" * 60)
    asyncio.run(main())