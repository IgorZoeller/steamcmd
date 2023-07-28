import asyncio
from io import TextIOWrapper
from collector import Downloadable
from subprocess import PIPE
import util.constants as constants


class Consumer:
    def __init__(self, queue : asyncio.PriorityQueue, id : str, item_map : dict = {}, cache : dict = {}, max_retry : int = 5):
        self.item_map   = item_map
        self.cache      = cache
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

    def cache_item(self, item):
        self.cache[item.item['id']] = self.cache.get(item.item['id'], 0) + 1
        return self.set_item_destination(item)

    def set_item_destination(self, item):
        if item.item['category'] == 'undefined':
            return item
        self.item_map[item.item['id']] = item.item['destination_folder']
        return item
    

    def remove_item_destination(self, item):
        del self.item_map[item.item['id']]


    async def consume(self):
        while True:
            item = await self.queue.get()
            self.get_count += 1
            await self.download_method(item)
            self.queue.task_done()
            self.task_count += 1

    async def reinsert_failed_items(self, stdout):
        while stdout.at_eof() == False:
            line = await stdout.readline()
            line = line.decode("utf-8").lower()
            if "rate limit exceeded" in line:
                print(f"[FATAL] {line} | Exiting program.")
                raise SystemExit
            elif "failed" in line: # ERROR! Download item {itemId} failed (Failure).
                lrs = line.rstrip().split(" ")
                itemId = lrs[-3]
                print(f"{line} | Putting {itemId} back in queue.")
                await self.queue.put(Downloadable(0, {"id" : itemId, "category" : "undefined"}))

    async def download_item(self, item):
        print(f"[{self.id.upper()}] - Consuming one more item. Remaining: {self.queue.qsize()}")
        self.cache_item(item)
        if self.cache[item.item['id']] > self.max_retry:
            print(f"Maximum retries ({self.max_retry}) reached for {item.item['id']}")
            self.fail_count += 1
            return -1
        print(f"[{self.id.upper()}] - Downloading item {item.item['id']} | Retry? {self.cache[item.item['id']] > 1}")
        process = await asyncio.create_subprocess_shell(
            f".\steamcmd\steamcmd.exe +login {constants.USERNAME} {constants.PASSWORD} +workshop_download_item 255710 {item.item['id']} +quit",
            stdin=PIPE, stdout=PIPE
        )
        await process.wait()
        asyncio.create_task(self.reinsert_failed_items(process.stdout))
        return 0

    async def download_multiple(self, item):
        item_list = [item]
        self.cache_item(item)
        qsize = self.queue.qsize()
        for _ in range(min(qsize, 50)):
            item_list.append(self.cache_item(await self.queue.get()))
            item_id = item_list[-1].item['id']
            if self.cache[item_id] > self.max_retry:
                print(f"Maximum retries ({self.max_retry}) reached for {item_id}")
                item_list.pop()
                self.queue.task_done()
                self.fail_count += 1
                continue
            print(f"[{self.id.upper()}] - Consuming one more item ({item_id}). Remaining: {self.queue.qsize()}")
            self.queue.task_done()
        cmd = ' '.join([f'+workshop_download_item 255710 {it.item["id"]}' for it in item_list])
        print(f"[{self.id.upper()}] - Downloading {len(item_list)} items.")
        process = await asyncio.create_subprocess_shell(
            f".\steamcmd\steamcmd.exe +login {constants.USERNAME} {constants.PASSWORD} {cmd} +quit",
            stdin=PIPE, stdout=PIPE
        )
        await process.wait()
        asyncio.create_task(self.reinsert_failed_items(process.stdout))
        return 0