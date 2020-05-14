#!/usr/bin/env python3

import datetime
import zlib
import websocket
import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

start_time = time.time()
epoch = datetime.datetime.utcfromtimestamp(0)

def inflate(data):
    decompress = zlib.decompressobj(-zlib.MAX_WBITS)
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated

def run_tbt():
    uri = "wss://real.OKEx.com:8443/ws/v3"
    ws = websocket.create_connection(uri)
    ws.send(json.dumps({'op': 'subscribe', 'args': ['spot/depth_l2_tbt:BTC-USDT']}))
    ws.recv()
    ws.recv()
    samples = []
    while time.time() - start_time < 60:
        data = json.loads(inflate(ws.recv()))
        time_ms = int(time.time() * 1000)
        for x in data['data']:
            exch_dt = datetime.datetime.strptime(x['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
            samples.append(time_ms - (exch_dt - epoch).total_seconds() * 1000.0)
    return samples

def run_100ms():
    uri = "wss://real.OKEx.com:8443/ws/v3"
    ws = websocket.create_connection(uri)
    ws.send(json.dumps({'op': 'subscribe', 'args': ['spot/depth:BTC-USDT']}))
    ws.recv()
    ws.recv()
    samples = []
    while time.time() - start_time < 60:
        data = json.loads(inflate(ws.recv()))
        time_ms = int(time.time() * 1000)
        for x in data['data']:
            exch_dt = datetime.datetime.strptime(x['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
            samples.append(time_ms - (exch_dt - epoch).total_seconds() * 1000.0)
    return samples

async def main():
    loop = asyncio.get_event_loop()
    futures = [loop.run_in_executor(None, run_tbt)]
    futures += [loop.run_in_executor(None, run_100ms)]
    await asyncio.wait(futures)

    print("tbt:")
    print(pd.Series(futures[0].result()).describe())
    print()

    print("100ms:")
    print(pd.Series(futures[1].result()).describe())
    print()

loop = asyncio.get_event_loop()
loop.set_default_executor(ThreadPoolExecutor(max_workers = 4,))
loop.run_until_complete(main())
