from __future__ import annotations

import asyncio
import sys
import time

from .infrastructure import assert_isolated
from .observer import install


async def main(action):
    cfg = assert_isolated()
    install()
    if action == "init":
        from engram.storage.postgres import PostgresStore
        from engram.storage.neo4j_store import Neo4jStore
        from engram.cli import cmd_migrate
        from argparse import Namespace
        print("Initializing isolated native schemas", flush=True)
        cmd_migrate(Namespace())
        neo = Neo4jStore(cfg.knowledge_graph)
        for _ in range(120):
            if neo.ping():
                break
            await asyncio.sleep(2)
        else:
            raise RuntimeError("Isolated Neo4j readiness timeout")
        neo.ensure_indexes()
        with neo.reader().session() as session:
            session.run("CALL db.awaitIndexes(300)").consume()
        neo.close()
        from temporalio.client import Client
        for _ in range(120):
            try:
                await Client.connect(cfg.temporal.address, namespace=cfg.temporal.namespace)
                return
            except Exception:
                await asyncio.sleep(2)
        raise RuntimeError("Isolated Temporal readiness timeout")
    if action == "worker":
        from engram.temporal.worker import run
        await run("all")
    elif action == "dispatcher":
        from engram.temporal.dispatcher import run
        await run()
    else:
        raise ValueError(action)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
