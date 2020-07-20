async def count(db, table, condition="1=1"):
    qs = "SELECT count(*) from {table} WHERE {condition}".format(
        table=table, condition=condition
    )
    return await db.fetchval(qs)
