from jobs.migrations import migrate
from pytest_docker_fixtures import images

import asyncio
import asyncpg
import pytest

images.configure("postgresql", "postgres", "11.1")


@pytest.yield_fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def db_settings():
    """
    detect travis, use travis's postgres; otherwise, use docker
    """
    import pytest_docker_fixtures

    host, port = pytest_docker_fixtures.pg_image.run()
    yield host, port
    pytest_docker_fixtures.pg_image.stop()


@pytest.fixture(scope="session")
async def db_conn(event_loop, db_settings):
    host, port = db_settings
    dsn = f"postgresql://postgres@{host}:{port}/guillotina"
    db = await asyncpg.connect(
        dsn, server_settings={"application_name": "pytest.fixture"}
    )

    def log_listener(conn, message):
        print(f"PG: {message}")

    db.add_log_listener(log_listener)
    await migrate(db)
    yield db


@pytest.fixture()
async def db(db_conn):
    txn = db_conn.transaction()
    await txn.start()
    yield db_conn
    await txn.rollback()
