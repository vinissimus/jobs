import asyncio
import asyncpg
import jobs
import logging
import sys

logger = logging.getLogger("jobs")


class Worker:
    def __init__(self, dsn, batch_size=1, wait=1, con_args=None):
        self.dsn = dsn
        self.conn_args = con_args or {}
        self.batch_size = batch_size
        self._con = None
        self.closing = False
        self.wait = wait

    async def work(self):
        conn = await self.get_connection()
        while True and not self.closing:
            try:
                tasks = await jobs.consume(conn, self.batch_size)
                for job in tasks:
                    await jobs.run(conn, job, sync=True)
                await asyncio.sleep(self.wait)
            except asyncio.CancelledError:
                await conn.close()
            except asyncpg.exceptions.ConnectionDoesNotExistError:
                conn = await self.get_connection(True)
        await conn.close()

    async def get_connection(self, refresh=False):
        if self._con is None or refresh:
            if "server_settings" not in self.conn_args:
                self.conn_args["server_settings"] = {}
            if "application_name" not in self.conn_args["server_settings"]:
                self.conn_args["server_settings"] = {
                    "application_name": "jobs-worker"
                }
            self._con = await asyncpg.connect(self.dsn, **self.conn_args)
        return self._con

    def close(self):
        self.closing = True


async def main(dsn: str, num_workers=1, **kwargs):
    tasks = []
    for w in range(0, num_workers):
        worker = Worker(dsn, **kwargs)
        tasks.append(worker.work())
    await asyncio.gather(*tasks)


def process_run(dsn: str, **kwargs):
    asyncio.run(main(dsn, **kwargs))


usage = """
    run it with:
    jobs-worker postgresql://xxx:xxxx@localhost:5432/db
"""


def run():
    if len(sys.argv) != 2:
        print(usage)
        sys.exit(1)

    asyncio.run(main())


if __name__ == "__main__":
    run()
