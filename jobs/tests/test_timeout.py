import pytest
import jobs
import asyncpg
import asyncio

pytestmark = pytest.mark.asyncio

"""
A job could be considered expired, when elapsed
time from it was pick up has expired...

"""


async def test_timeout_ontasks(db):
    await jobs.publish(db, "atask", timeout=1)
    [task] = await jobs.consume(db, 1)
    await asyncio.sleep(0.1)

