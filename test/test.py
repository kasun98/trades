import asyncio
import websockets
import json
from datetime import datetime

async def get_btc_price():
    url = "wss://ws.kraken.com/v2"

    async with websockets.connect(url) as websocket:
        # Subscribe to BTC/USD ticker feed
        subscribe_msg = {
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": ["BTC/USD"]
            }
        }

        await websocket.send(json.dumps(subscribe_msg))
        print("Subscribed to BTC/USD ticker.")

        while True:
            message = await websocket.recv()
            data = json.loads(message)

            # Price data usually comes in 'ticker' updates
            if isinstance(data, dict) and data.get("channel") == "ticker":
                price = data["data"][0]["bid"]
                print(data["data"][0])
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                # print(f"Live BTC/USD bid Price: {price} USD  at {current_time}")

# Run the async function
asyncio.run(get_btc_price())
