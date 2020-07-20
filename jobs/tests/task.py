# some tasks to do tests

import asyncpg
import asyncio


async def task(num, num2):
    return num + num2


async def long_task(num, num2):
    await asyncio.sleep(1)
    return num + num2
