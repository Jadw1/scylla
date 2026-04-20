#!/usr/bin/env python3
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from __future__ import annotations

import asyncio
import logging
import shlex
import subprocess
from pathlib import Path

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient


logger = logging.getLogger(__name__)


DEFAULT_CONFIG = {"experimental_features": ["strongly-consistent-tables"]}
DEFAULT_CMDLINE = [
    "--logger-log-level", "sc_groups_manager=debug",
    "--logger-log-level", "sc_coordinator=debug",
    "--smp", "6"
]

STRONG_CONSISTENCY_METRICS = (
    "scylla_strong_consistency_write_node_bounces",
    "scylla_strong_consistency_write_shard_bounces",
    "scylla_strong_consistency_read_node_bounces",
    "scylla_strong_consistency_read_shard_bounces",
)

FORWARDING_METRICS = (
    "scylla_cql_forwarded_requests",
    "scylla_transport_requests_forwarded_successfully",
    "scylla_transport_requests_forwarded_redirected",
    "scylla_transport_requests_forwarded_failed",
    "scylla_transport_requests_forwarded_prepared_not_found",
)

ALL_RELEVANT_METRICS = STRONG_CONSISTENCY_METRICS + FORWARDING_METRICS


# Local config. Edit these values directly.
CQL_STRESS_BINARY = Path(
    "~/src/cql-stress/target/debug/cql-stress-cassandra-stress"
).expanduser()

CQL_STRESS_DURATION = "30s"
CQL_STRESS_WRITE_RATE = 5000
CQL_STRESS_THREADS = 50
CQL_STRESS_RF = 3
CQL_STRESS_CONSISTENCY = "ONE"
CQL_STRESS_TIMEOUT_SECONDS = 900
CQL_STRESS_PRE_RUN_SLEEP_SECONDS = 5

CQL_STRESS_WRITE_EXTRA_ARGS: tuple[str, ...] = ()


async def _run_subprocess(
    args: list[str],
    *,
    cwd: Path | None = None,
    timeout_seconds: int = 600,
) -> subprocess.CompletedProcess[str]:
    loop = asyncio.get_running_loop()

    def _run() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            args,
            cwd=cwd,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )

    result = await loop.run_in_executor(None, _run)
    logger.info("Command finished: %s", shlex.join(args))
    if result.stdout:
        logger.info("stdout:\n%s", result.stdout)
    if result.stderr:
        logger.info("stderr:\n%s", result.stderr)
    if result.returncode != 0:
        raise AssertionError(
            f"Command failed with exit code {result.returncode}: {shlex.join(args)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


async def _ensure_local_cql_stress_binary() -> Path:
    if CQL_STRESS_BINARY.exists():
        return CQL_STRESS_BINARY

    raise AssertionError(
        f"Local cql-stress binary not found at {CQL_STRESS_BINARY}. "
        "Build it manually or edit CQL_STRESS_BINARY in this file."
    )


def _metrics_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {name: after[name] - before[name] for name in before}


def _format_metrics(metrics: dict[str, int]) -> str:
    return "\n".join(f"  {name}: {value}" for name, value in sorted(metrics.items()))


async def _collect_cluster_metrics(manager: ManagerClient, servers) -> dict[str, int]:
    snapshots = await asyncio.gather(*(manager.metrics.query(server.ip_addr) for server in servers))
    totals = {name: 0 for name in ALL_RELEVANT_METRICS}
    for snapshot in snapshots:
        for name in totals:
            totals[name] += snapshot.get(name) or 0
    return totals


async def _assert_standard1_tablets_have_expected_rf(manager: ManagerClient, keyspace: str, expected_rf: int) -> None:
    table_id = await manager.get_table_id(keyspace, "standard1")
    rows = await manager.get_cql().run_async(
        f"SELECT last_token, raft_group_id, replicas FROM system.tablets WHERE table_id = {table_id}"
    )

    assert rows, f"Expected at least one tablet for {keyspace}.standard1"

    raft_groups = {str(row.raft_group_id) for row in rows}
    assert len(raft_groups) == len(rows), (
        "Expected each tablet to have its own raft group, "
        f"got {len(raft_groups)} unique groups for {len(rows)} tablets"
    )

    replica_counts = {len(row.replicas) for row in rows}
    assert replica_counts == {expected_rf}, (
        f"Expected every tablet to have RF={expected_rf}, got replica counts {sorted(replica_counts)}"
    )

    logger.info(
        "Tablet layout for %s.standard1: %d tablets, %d unique raft groups, RF=%d",
        keyspace,
        len(rows),
        len(raft_groups),
        expected_rf,
    )


def _cql_stress_command(
    binary: Path,
    hosts: list[str],
    *,
    keyspace: str,
    workload: str,
    extra_args: tuple[str, ...],
) -> list[str]:
    return [
        str(binary),
        workload,
        f"duration={CQL_STRESS_DURATION}",
        "no-warmup",
        f"cl={CQL_STRESS_CONSISTENCY}",
        "-rate",
        f"threads={CQL_STRESS_THREADS}",
        f"throttle={CQL_STRESS_WRITE_RATE}/s",
        "-schema",
        f"replication(strategy=NetworkTopologyStrategy,factor={CQL_STRESS_RF})",
        f"keyspace={keyspace}",
        "-node",
        ",".join(hosts),
        *extra_args,
    ]


async def _run_cql_stress(
    binary: Path,
    hosts: list[str],
    *,
    keyspace: str,
    workload: str,
    extra_args: tuple[str, ...],
) -> None:
    cmd = _cql_stress_command(
        binary,
        hosts,
        keyspace=keyspace,
        workload=workload,
        extra_args=extra_args,
    )
    await _run_subprocess(cmd, timeout_seconds=CQL_STRESS_TIMEOUT_SECONDS)


def _assert_direct_routing(delta: dict[str, int], workload: str) -> None:
    if workload == "write":
        keys = (
            "scylla_strong_consistency_write_node_bounces",
            "scylla_strong_consistency_write_shard_bounces",
        )
    else:
        keys = (
            "scylla_strong_consistency_read_node_bounces",
            "scylla_strong_consistency_read_shard_bounces",
        )

    keys += FORWARDING_METRICS

    failures = {name: delta[name] for name in keys if delta[name] != 0}
    assert not failures, (
        f"Detected non-direct strong consistency {workload} routing while running cql-stress.\n"
        f"Relevant deltas:\n{_format_metrics(delta)}"
    )


@pytest.mark.manual
@pytest.mark.slow
@pytest.mark.asyncio
async def test_cql_stress_routes_without_strong_consistency_bounces(manager: ManagerClient):
    """Manual regression test for local cql-stress strong-consistency routing."""
    binary = await _ensure_local_cql_stress_binary()

    logger.info("Bootstrapping cluster for cql-stress routing test")
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc="dc1")
    await manager.get_ready_cql(servers)

    node_ips = [server.ip_addr for server in servers]
    keyspace_opts = (
        f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {CQL_STRESS_RF}}} "
        "AND consistency = 'global'"
    )

    async with new_test_keyspace(manager, keyspace_opts) as keyspace:
        await asyncio.sleep(CQL_STRESS_PRE_RUN_SLEEP_SECONDS)

        stress_success = True
        try:
            await _run_cql_stress(
                binary,
                node_ips,
                keyspace=keyspace,
                workload="write",
                extra_args=CQL_STRESS_WRITE_EXTRA_ARGS,
            )
        except:
            stress_success = False
        await _assert_standard1_tablets_have_expected_rf(manager, keyspace, CQL_STRESS_RF)
        await asyncio.sleep(5)
        
        for server in servers:
            metrics = await manager.metrics.query(server.ip_addr)
            write_shard_bounces = metrics.get('scylla_strong_consistency_write_shard_bounces') or 0
            assert write_shard_bounces == 0
            write_node_bounces = metrics.get('scylla_strong_consistency_write_node_bounces') or 0
            assert write_node_bounces == 0
            read_node_bounces = metrics.get('scylla_strong_consistency_read_node_bounces') or 0
            assert read_node_bounces == 0
            read_shard_bounces = metrics.get('scylla_strong_consistency_read_shard_bounces') or 0
            assert read_shard_bounces == 0
        assert stress_success

