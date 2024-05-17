import asyncio
import json
import websockets
from fastapi import FastAPI
from ariadne import QueryType, SubscriptionType, make_executable_schema, gql
from ariadne.asgi import GraphQL
from ariadne.asgi.handlers import GraphQLWSHandler
from broadcaster import Broadcast
import uvicorn
from pprint import pprint

app = FastAPI()
broadcast = Broadcast("redis://localhost:6379")

type_defs = gql("""
    
    type Query {
        hell: String
    }

   
""")

query = QueryType()
# subscription = SubscriptionType()

@query.field("hell")
def resolve_hello(*_):
    return "Hello, world!"

# @subscription.source("aisMessageReceived")
# async def subscribe_ais_message(_, info):
#     print("Subscribing to AIS messages...")
#     async with broadcast.subscribe(channel="ais_messages") as subscriber:
#         print("Subscription established for AIS messages.")
#         async for event in subscriber:
#             message = event.message
#             ship_name = message.get('MetaData', {}).get('ShipName')
#             if ship_name:
#                 print(f"Received AIS message for Ship: {ship_name}")
#                 yield ship_name
#             else:
#                 print("Received AIS message without ShipName.")

# @subscription.field("aisMessageReceived")
# def resolve_ais_message(message, *_):
#     return message

schema = make_executable_schema(type_defs, query)

graphql = GraphQL(
    schema=schema,
    debug=True,
    websocket_handler=GraphQLWSHandler(),
)





async def connect_ais_stream():
    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {
            "APIKey": "15e78b22d9b80cd996a10e1c12e12025fb16c8f7",
            "BoundingBoxes": [[[-90, -180], [90, 180]]]
        }
        subscribe_message_json = json.dumps(subscribe_message)
        await websocket.send(subscribe_message_json)
        print("-----")

        try:
            async for message_json in websocket:
                message = json.loads(message_json)
                # pprint(message)
                # print("Connecting to the broadcaster to publish the message")
                await broadcast_ais_message(message)
        except websockets.exceptions.ConnectionClosedError:
            print("Connection closed unexpectedly.")
        except asyncio.exceptions.IncompleteReadError:
            print("Incomplete read error occurred.")
        except json.JSONDecodeError:
            print("Error decoding JSON message.")

print("---")
async def broadcast_ais_message(message):
    async with broadcast:
        await broadcast.publish(channel="ais_messages", message=json.dumps(message))

print("----")

@app.on_event("startup")
async def startup_event():
    await broadcast.connect()
    asyncio.create_task(connect_ais_stream())
    # await asyncio.gather(connect_ais_stream(), start_redis_server())
    

@app.on_event("shutdown")
async def shutdown_event():
    await broadcast.disconnect()

app.mount("/graphql", graphql)
#app.add_websocket_route("/graphql/", graphql)

# if __name__ == "__main__":
#     import uvicorn
#     asyncio.run(app.startup())
#     uvicorn.run(app, host="0.0.0.0", port=8000)

