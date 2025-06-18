#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import datetime
import logging
from concurrent.futures.thread import ThreadPoolExecutor

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args

from test.cluster.dtest.dtest_class import create_cf


logger = logging.getLogger(__name__)


def create_c1c2_table(session, cf="cf", read_repair=None, debug_query=True, compaction=None, caching=True, speculative_retry=None):  # noqa: PLR0913
    create_cf(session, cf, columns={"c1": "text", "c2": "text"}, read_repair=read_repair, debug_query=debug_query, compaction=compaction, caching=caching, speculative_retry=speculative_retry)


def insert_c1c2(  # noqa: PLR0913
    session,
    keys=None,
    n=None,
    consistency=ConsistencyLevel.QUORUM,
    c1_values=None,
    c2_values=None,
    ks="ks",
    cf="cf",
    concurrency=20,
):
    if (keys is None and n is None) or (keys is not None and n is not None):
        raise ValueError(f"Expected exactly one of 'keys' or 'n' arguments to not be None; got keys={keys}, n={n}")
    if (not c1_values and c2_values) or (c1_values and not c2_values):
        raise ValueError('Expected the "c1_values" and "c2_values" variables be empty or contain list of string')
    if n:
        keys = list(range(n))
    if c1_values and c2_values:
        statement = session.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, ?, ?)")
        statement.consistency_level = consistency
        execute_concurrent_with_args(session, statement, map(lambda x, y, z: [f"k{x}", y, z], keys, c1_values, c2_values), concurrency=concurrency)
    else:
        statement = session.prepare(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES (?, 'value1', 'value2')")
        statement.consistency_level = consistency

        execute_concurrent_with_args(session, statement, [[f"k{k}"] for k in keys], concurrency=concurrency)


def rows_to_list(rows):
    new_list = [list(row) for row in rows]
    return new_list


def get_list_res(session, query, cl, ignore_order=False, result_as_string=False, timeout=None):
    simple_query = SimpleStatement(query, consistency_level=cl)
    if timeout is not None:
        res = session.execute(simple_query, timeout=timeout)
    else:
        res = session.execute(simple_query)
    list_res = rows_to_list(res)
    if ignore_order:
        list_res = sorted(list_res)
    if result_as_string:
        list_res = str(list_res)
    return list_res


def run_in_parallel(functions_list):
    """
    Runs the functions that are passed in proc_functions in parallel using threads.
    :param functions_list: variable holds list of dictionaries with threads definitions. Expected structure:
                           [{'func': <function pointer - the function will be runs from the thread>,
                             'args': (arg1, arg2, arg3), - explicit function arguments by order in the function
                             'kwargs': {<arg name1>: value, <arg name2>: value} - function arguments by name
                            }, - first thread definition
                            {{'func': <function pointer, 'args': (), 'kwargs': {}} - second thread, no arguments
                           ]
    :param functions_list: list
    :return: list of functions' return values
    :rtype: list
    """
    logger.debug(f"Threads start at {datetime.datetime.now()}")
    pool = ThreadPoolExecutor(max_workers=len(functions_list))
    tasks = []
    for func in functions_list:
        args = func["args"] if "args" in func else []
        kwargs = func["kwargs"] if "kwargs" in func else {}
        tasks.append(pool.submit(func["func"], *args, **kwargs))
    results = [task.result() for task in tasks]
    logger.debug(f"'{len(results)}' threads finished at {datetime.datetime.now()}")
    return results
