from consumer import Consumer
import util.constants as constants
import collector
import asyncio
import os
import sys
import shutil
import time

NUMBER_OF_CONSUMERS = 1

queue = asyncio.PriorityQueue()

async def gather(producer):
    while True:
        if producer.done(): return
        await asyncio.sleep(0.5)


async def main():
    producer = asyncio.create_task(asyncio.to_thread(collector.collect, sys.argv[1], queue))
    consumers = [Consumer(queue, f'consumer_0{i}') for i in range(NUMBER_OF_CONSUMERS)]
    
    task_consume = [asyncio.create_task(c.consume()) for c in consumers]
    start = time.monotonic()
    await asyncio.ensure_future(gather(producer))
    print(f'All {producer.result()} queued for download.')
    for c in consumers:
        c.toggle_download_method()
    await queue.join()
    print("> All items downloaded.")
    for c in task_consume:
        c.cancel()

    print(f"Start moving {len(consumers[0].item_map.keys())} items...")
    for k_code,v_dst in consumers[0].item_map.items():
        item_folder = os.path.join(constants.SOURCE_FOLDER, k_code)
        try:
            if os.path.exists(v_dst):
                shutil.rmtree(os.path.join(v_dst, k_code))
            print(f"# Moving {item_folder} ==> {shutil.move(item_folder, v_dst)}")
        except:
            print(f'! Failed to move {item_folder}\n    ==> {v_dst}')
    end = time.monotonic()
    print(f"Computation finished. Program took {end - start} seconds ({int((end - start)/60)} minutes) to run.")
            

asyncio.run(main())