#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import random
import pytest
import logging
import time
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier, inject_error_one_shot
from test.pylib.util import wait_for
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)

async def wait_for_view_is_built(cql, ks, view):
    async def check_table():
        result = await cql.run_async(f"SELECT * FROM system.built_tablet_views WHERE keyspace_name='{ks}' AND view_name='{view}'")
        if len(result) == 1:
            return True
        else:
            return None
    await wait_for(check_table, time.time() + 60)

async def get_tablet_count(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str):
    host = manager.cql.cluster.metadata.get_host(server.ip_addr)

    # read_barrier is needed to ensure that local tablet metadata on the queried node
    # reflects the finalized tablet movement.
    await read_barrier(manager.api, server.ip_addr)

    table_id = await manager.get_table_id(keyspace_name, table_name)
    rows = await manager.cql.run_async(f"SELECT tablet_count FROM system.tablets where "
                                       f"table_id = {table_id}", host=host)
    return rows[0].tablet_count

async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, False) for s in servers]
    await asyncio.gather(*errs)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_view_building(manager: ManagerClient):
    servers = await manager.servers_add(2)
    cql = manager.get_cql()

    async def validate_view_is_built(replication_factor, rows):
        ks = f"ks{replication_factor}"
        await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}")
        await cql.run_async(f"CREATE TABLE {ks}.tbl(id int primary key, v1 int, v2 int)")
        stmt = cql.prepare(f"INSERT INTO {ks}.tbl(id, v1, v2) VALUES (?, ?, ?)")
        for i in range(rows):
            await cql.run_async(stmt, [i, i, i])
        
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.tbl WHERE v1 IS NOT NULL primary key(id, v1)")
        await wait_for_view_is_built(cql, ks, "mv")
        
        tasks_count = (await cql.run_async(f"SELECT count(*) FROM system.view_building_coordinator_tasks"))[0].count
        assert tasks_count == 0

        rows_in_view = (await cql.run_async(f"SELECT count(*) FROM {ks}.mv"))[0].count
        assert rows == rows_in_view
    
    await validate_view_is_built(1, 1024)
    await validate_view_is_built(2, 1024)

@pytest.mark.asyncio
async def test_view_building_with_tablet_split(manager: ManagerClient):
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'table=debug',
        '--logger-log-level', 'load_balancer=debug',
        '--target-tablet-size-in-bytes', '10000',
    ]
    server = await manager.server_add(config={
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
    }, cmdline=cmdline)
    cql = manager.get_cql()

    await manager.api.disable_tablet_balancing(server.ip_addr)

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1;")

    keys = 250
    insert_stmt = cql.prepare(f"INSERT INTO test.test(pk, c) VALUES (?, ?)")
    for pk in range(keys):
        await cql.run_async(insert_stmt, [pk, random.randbytes(100)])

    await cql.run_async("CREATE MATERIALIZED VIEW test.mv AS SELECT * FROM test.test WHERE c IS NOT NULL primary key(pk, c)")
    tablet_count = await get_tablet_count(manager, server, 'test', 'test')
    assert tablet_count == 1
    view_tablet_count = await get_tablet_count(manager, server, 'test', 'mv')
    assert view_tablet_count == 1

    await manager.api.flush_keyspace(server.ip_addr, "test")
    await inject_error_one_shot(manager.api, server.ip_addr, "tablet_allocator_shuffle")
    await inject_error_on(manager, "tablet_load_stats_refresh_before_rebalancing", server)

    s1_log = await manager.server_open_log(server.server_id)
    s1_mark = await s1_log.mark()

    await manager.api.enable_tablet_balancing(server.ip_addr)

    time.sleep(10)
    # await s1_log.wait_for('Detected tablet split for table', from_mark=s1_mark, timeout=5*60)

    tablet_count = await get_tablet_count(manager, server, 'test', 'test')
    print(f"\n\nTABLETS AFTER BALANCING: {tablet_count}\n\n")

    assert False

    
