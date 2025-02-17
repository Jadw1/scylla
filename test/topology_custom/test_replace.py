#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test replacing node in different scenarios
"""
import time
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.topology.util import wait_for_token_ring_and_group0_consistency, wait_for_cql_and_get_hosts, wait_for
import pytest
import logging


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_replace_different_ip(manager: ManagerClient) -> None:
    """Replace an existing node with new node using a different IP address"""
    servers = await manager.servers_add(3, config={'failure_detector_timeout_in_ms': 2000})
    logger.info(f"cluster started, servers {servers}")

    logger.info(f"replacing server {servers[0]}")
    await manager.server_stop(servers[0].server_id)
    replaced_server = servers[0]
    replace_cfg = ReplaceConfig(replaced_id = replaced_server.server_id, reuse_ip_addr = False, use_host_id = False)
    new_server = await manager.server_add(replace_cfg)
    cql = manager.get_cql()
    servers = await manager.running_servers()
    all_ips = set([s.rpc_address for s in servers])
    logger.info(f"new server {new_server} started, all ips {all_ips}, "
                "waiting for token ring and group0 consistency")
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    for s in servers:
        peers_to_see = all_ips - {s.rpc_address}

        logger.info(f'waiting for cql and get hosts for {s}')
        h = (await wait_for_cql_and_get_hosts(cql, [s], time.time() + 60))[0]

        logger.info(f"waiting for {s} to see its peers {peers_to_see}")
        async def check_peers_and_gossiper():
            peers = set([r.peer for r in await cql.run_async("select peer from system.peers", host=h)])
            remaining = peers_to_see - peers
            if remaining:
                logger.info(f"server {h} doesn't see its peers, all_ips {all_ips}, peers_to_see {peers_to_see}, remaining {remaining}, continue waiting")
                return None

            alive_eps = await manager.api.get_alive_endpoints(s.ip_addr)
            if replaced_server.ip_addr in alive_eps:
                logger.info(f"server {h}, replaced ip {replaced_server.ip_addr} is contained in alive eps {alive_eps}, continue waiting")
                return None

            down_eps = await manager.api.get_down_endpoints(s.ip_addr)
            if replaced_server.ip_addr in down_eps:
                logger.info(f"server {h}, replaced ip {replaced_server.ip_addr} is contained in down eps {down_eps}, continue waiting")
                return None

            return True
        await wait_for(check_peers_and_gossiper, time.time() + 60)
        logger.info(f"server {s} system.peers and gossiper state is valid")

@pytest.mark.asyncio
async def test_replace_different_ip_using_host_id(manager: ManagerClient) -> None:
    """Replace an existing node with new node reusing the replaced node host id"""
    servers = await manager.servers_add(3, config={'failure_detector_timeout_in_ms': 2000})
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = True)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

@pytest.mark.asyncio
async def test_replace_reuse_ip(manager: ManagerClient) -> None:
    """Replace an existing node with new node using the same IP address"""
    servers = await manager.servers_add(3, config={'failure_detector_timeout_in_ms': 2000})
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = True, use_host_id = False)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

@pytest.mark.asyncio
async def test_replace_reuse_ip_using_host_id(manager: ManagerClient) -> None:
    """Replace an existing node with new node using the same IP address and same host id"""
    servers = await manager.servers_add(3, config={'failure_detector_timeout_in_ms': 2000})
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = True, use_host_id = True)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
