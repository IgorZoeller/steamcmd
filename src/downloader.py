from subprocess import PIPE
from collector import Downloadable
import asyncio
import os
import collector
import sys
import util.constants as constants
import shutil

NUMBER_OF_CONSUMERS = 1
MAX_RETRY = 5
get_count  = 0
task_count = 0
fail_count = 0

queue = asyncio.PriorityQueue()
item_map = {}

async def download_item(_item : Downloadable, consumer_id : str, retry = 0):
    global get_count, fail_count
    _stdout = PIPE
    if retry == MAX_RETRY:
        _stdout = sys.stdout
    if retry > MAX_RETRY:
        del item_map[_item.item['id']]
        fail_count += 1
        return
    await asyncio.sleep(1.25 * retry)
    print(f"[{consumer_id.upper()}] > Downloading item {_item.item['id']} | Retry? {retry > 0}")
    process = await asyncio.create_subprocess_shell(
        f"steamcmd +login {constants.USERNAME} {constants.PASSWORD} +workshop_download_item 255710 {_item.item['id']} +quit",
        stdin=PIPE, stdout=_stdout
    )
    await process.wait()
    if (len([n for n in os.listdir(constants.SOURCE_FOLDER)]) + fail_count) < get_count:
        await download_item(_item, consumer_id, retry + 1)


async def consumer(queue : asyncio.PriorityQueue, consumer_id : str):
    global get_count, task_count

    print(f'+ New consumer [{consumer_id.upper()}]')

    while True:
        task = await queue.get()
        if task.item['category'] != 'undefined':
            item_map[task.item['id']] = task.item['destination_folder']
        get_count += 1
        print(f"[{consumer_id.upper()}] - Consuming one more item. Remaining: {queue.qsize()}")
        await download_item(task, consumer_id)
        queue.task_done()
        task_count += 1
        print(f'[DEBUG] {get_count=} | {task_count=} | {fail_count=}')

def create_download_script(queue : asyncio.PriorityQueue):
    item_list = []
    for _ in range(queue.qsize()):
        item = queue.get_nowait()
        queue.task_done()
        item_list.append(item.item['id'])

async def gather(producer):
    while True:
        if producer.done(): return
        await asyncio.sleep(0.5)


async def main():
    producer = asyncio.create_task(asyncio.to_thread(collector.collect, sys.argv[1], queue))
    consumers = [asyncio.create_task(consumer(queue, f'consumer_0{i}')) for i in range(NUMBER_OF_CONSUMERS)]

    await asyncio.ensure_future(gather(producer))
    print(f'All {producer.result()} queued for download.')
    await queue.join()
    print("> All items downloaded.")

    for c in consumers:
        c.cancel()

    for k_code,v_dst in item_map.items():
        item_folder = os.path.join(constants.SOURCE_FOLDER, k_code)
        try:
            shutil.rmtree(os.path.join(v_dst, k_code))
        finally:
            print(f"# Moving {item_folder} ==> {shutil.move(item_folder, v_dst)}")

asyncio.run(main())