from pathlib import Path

import asyncio
import asyncpg
import glob
import logging
import sys
import typing

logger = logging.getLogger("jobs")

current = Path(__file__)


def get_migrations_path() -> Path:
    return current.parent / "sql"


def get_available():
    files: typing.Dict[int, str] = {}
    path = str(get_migrations_path())
    for item in glob.glob(f"{path}/*.up.sql"):
        file = item.replace(path + "/", "")
        version = int(file.split("_")[0])
        files[version] = file
    return files


def load_migration(name: str):
    file = get_migrations_path() / name
    with file.open() as f:
        return f.read()


async def migrate(db: asyncpg.Connection = None):
    migrations = get_available()
    try:
        current = await db.fetchval("SELECT migration FROM jobs.migrations")
    except asyncpg.exceptions.UndefinedTableError:
        current = 0
    logger.info("Current migration %s", current)

    applied = current
    async with db.transaction():
        for avail in sorted(list(migrations.keys())):
            if avail > current:
                logger.info("Appling migration %s", migrations[avail])
                data = load_migration(migrations[avail])
                await db.execute(data)
                applied = avail

        if applied != current:
            logger.info("Update migrations history version: %s", applied)
            await db.execute("update jobs.migrations set migration=$1", applied)
        else:
            logger.info("No migrations applied. Your db it's at latest version")


async def main(dsn: str):
    db = await asyncpg.connection(dsn=dsn)
    await migrate(db)


usage = """
    run it with:
    job-migrations postgresql://xxx:xxxx@localhost:5432/db
"""


def run():
    if len(sys.argv) != 2:
        print(usage)
        sys.exit(1)

    asyncio.run(main())


if __name__ == "__main__":
    run()
