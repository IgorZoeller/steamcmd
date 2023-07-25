import asyncio
import os
import sys
from subprocess import PIPE
import util.constants as constants


class Consumer:
    def __init__(self, queue : asyncio.PriorityQueue, id : str, item_map : dict = {}, max_retry : int = 5):
        self.item_map   = item_map
        self.queue      = queue
        self.id         = id
        self.max_retry  = max_retry
        self.download_method = self.download_item
        self.get_count  = 0
        self.task_count = 0
        self.fail_count = 0
        print(f'+ New consumer [{self.id.upper()}]')


    def toggle_download_method(self):
        previous = self.download_method.__name__
        self.download_method = self.download_multiple if self.download_method == self.download_item else self.download_item
        current = self.download_method.__name__
        print(f'[{self.id.upper()}] - Changing download method from {previous} to {current}.')


    def set_item_destination(self, item):
        if item.item['category'] == 'undefined':
            return item
        self.item_map[item.item['id']] = item.item['destination_folder']
        return item
    

    def remove_item(self, item):
        del self.item_map[item.item['id']]


    async def consume(self):
        while True:
            item = await self.queue.get()
            self.get_count += 1
            await self.download_method(item)
            self.queue.task_done()
            self.task_count += 1


    async def download_item(self, item, retry = 0):
        print(f"[{self.id.upper()}] - Consuming one more item. Remaining: {self.queue.qsize()}")
        _stdout = PIPE
        if retry == 0:
            self.set_item_destination(item)
        elif retry == self.max_retry:
            _stdout = sys.stdout
        elif retry > self.max_retry:
            self.fail_count += 1
            self.task_count -= 1
            self.remove_item(item)
            return -1
        await asyncio.sleep(10 * retry)
        print(f"[{self.id.upper()}] - Downloading item {item.item['id']} | Retry? {retry > 0}")
        process = await asyncio.create_subprocess_shell(
            f".\steamcmd\steamcmd.exe +login {constants.USERNAME} {constants.PASSWORD} +workshop_download_item 255710 {item.item['id']} +quit",
            stdin=PIPE, stdout=_stdout
        )
        await process.wait()
        if (len([n for n in os.listdir(constants.SOURCE_FOLDER)]) + self.fail_count) < self.get_count:
            return await self.download_item(item, retry + 1)
        return 0
    

    async def download_multiple(self, item, retry = 0):
        item_list = [item]
        self.set_item_destination(item)
        qsize = self.queue.qsize()
        for _ in range(min(qsize, 50)):
            item_list.append(self.set_item_destination(await self.queue.get()))
            print(f"[{self.id.upper()}] - Consuming one more item. Remaining: {self.queue.qsize()}")
            self.queue.task_done()
        cmd = ' '.join([f'+workshop_download_item 255710 {it.item["id"]}' for it in item_list])
        print(f"[{self.id.upper()}] - Downloading {len(item_list)} items. | Retry? {retry > 0}")
        process = await asyncio.create_subprocess_shell(
            f".\steamcmd\steamcmd.exe +login {constants.USERNAME} {constants.PASSWORD} {cmd} +quit",
            stdin=PIPE, stdout=PIPE
        )
        await process.wait()
        return 0