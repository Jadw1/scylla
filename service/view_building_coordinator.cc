/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <exception>
#include <iterator>
#include <ranges>
#include <seastar/core/coroutine.hh>

#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/schema_tables.hh"
#include "db/system_keyspace.hh"
#include "schema/schema_fwd.hh"
#include "seastar/core/loop.hh"
#include "seastar/coroutine/maybe_yield.hh"
#include "service/query_state.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_group0_client.hh"
#include "utils/assert.hh"
#include "utils/log.hh"
#include "seastar/core/abort_source.hh"
#include "seastar/core/with_scheduling_group.hh"
#include "service/migration_manager.hh"
#include "replica/database.hh"
#include "view_info.hh"

#include "service/view_building_coordinator.hh"

logging::logger vbc_logger("vb_coordinator");

namespace service {

namespace vbc {

query_state& vb_coordinator_query_state() {
    using namespace std::chrono_literals;
    const auto t = 1h;
    static timeout_config tc{ t, t, t, t, t, t, t };
    static thread_local client_state cs(client_state::internal_tag{}, tc);
    static thread_local query_state qs(cs, empty_service_permit());
    return qs;
}

struct vbc_state {
    vbc_tasks tasks;
};

class view_building_coordinator : public migration_listener::only_view_notifications {
    replica::database& _db;
    raft_group0& _group0;
    db::system_keyspace& _sys_ks;
    const topology_state_machine& _topo_sm;
    
    abort_source& _as;
    condition_variable _cond;
    std::optional<vbc_state> _state;

public:
    view_building_coordinator(abort_source& as, replica::database& db, raft_group0& group0, db::system_keyspace& sys_ks, const topology_state_machine& topo_sm) 
        : _db(db)
        , _group0(group0)
        , _sys_ks(sys_ks)
        , _topo_sm(topo_sm)
        , _as(as) 
        , _state(std::nullopt)
    {}

    future<> run();

    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override { _cond.broadcast(); }
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {}
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override { _cond.broadcast(); }

private:
    future<group0_guard> start_operation() {
        auto guard = co_await _group0.client().start_operation(_as);
        co_return std::move(guard);
    }

    future<> await_event() {
        _as.check();
        co_await _cond.when();
        vbc_logger.debug("event awaited");
    }

    future<> initialize_coordinator_state() {
        auto tasks = co_await _sys_ks.get_view_building_coordinator_tasks();
        _state = vbc_state {
            .tasks = std::move(tasks)
        };
    }

    future<> update_coordinator_state(group0_guard guard);

    future<> add_view(const view_name& view_name, vbc_state& state_copy, group0_batch& batch);
    future<> remove_view(const view_name& view_name, vbc_state& state_copy, group0_batch& batch);

    table_id get_base_id(const view_name& view_name) {
        return _db.find_schema(view_name.first, view_name.second)->view_info()->base_id();
    }

private:
    future<std::set<view_name>> load_all_views() {
        static const sstring query = format("SELECT keyspace_name, view_name FROM {}.{}", db::schema_tables::v3::NAME, db::schema_tables::v3::VIEWS);

        std::set<view_name> views;
        co_await _sys_ks.query_processor().query_internal(query, [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
            auto ks_name = row.get_as<sstring>("keyspace_name");
            auto view_name = row.get_as<sstring>("view_name");

            views.insert({ks_name, view_name});
            co_return stop_iteration::no;
        });
        co_return views;
    }

    future<std::set<view_name>> load_built_views() {
        auto built_views = co_await _sys_ks.load_built_views();
        co_return std::set<view_name>(std::make_move_iterator(built_views.begin()), std::make_move_iterator(built_views.end()));
    }
};

future<> view_building_coordinator::run() {
    auto abort = _as.subscribe([this] noexcept {
        _cond.broadcast();
    });

    co_await initialize_coordinator_state();

    while (!_as.abort_requested()) {
        vbc_logger.debug("coordinator loop iteration");
        try {
            co_await update_coordinator_state(co_await start_operation());

            // TODO
            // Do actual work, send RPCs to build a particular view's range
            co_await await_event();
        } catch (...) {
            
        }
        co_await coroutine::maybe_yield();
    }
}

future<> view_building_coordinator::update_coordinator_state(group0_guard guard) {
    SCYLLA_ASSERT(_state);
    vbc_logger.debug("update_coordinator_state()");

    auto views = co_await load_all_views();
    auto built_views = co_await load_built_views();

    vbc_state state_copy = *_state;
    group0_batch batch(std::move(guard));

    for (auto& view: views) {
        if (!_db.find_keyspace(view.first).uses_tablets() || built_views.contains(view)) {
            continue;
        }
        auto base_id = get_base_id(view);
        if (!_state->tasks.contains(base_id) || !_state->tasks[base_id].contains(view)) {
            co_await add_view(view, state_copy, batch);
        }
    }

    for (auto& [_, view_tasks]: _state->tasks) {
        for (auto& [view, _]: view_tasks) {
            if (!views.contains(view)) {
                co_await remove_view(view, state_copy, batch);
            }
        }
    }

    if (!batch.empty()) {
        co_await std::move(batch).commit(_group0.client(), _as, std::nullopt); //TODO: specify timeout?
        _state = std::move(state_copy);
    }
}

future<> view_building_coordinator::add_view(const view_name& view_name, vbc_state& state_copy, group0_batch& batch) {
    vbc_logger.info("Register new view: {}.{}", view_name.first, view_name.second);
    static const sstring query = format("INSERT INTO {}.{}(keyspace_name, view_name, host_id, shard, start_token, end_token) VALUES (?, ?, ?, ?, ?, ?)", db::system_keyspace::NAME, db::system_keyspace::VIEW_BUILDING_COORDINATOR_TASKS);

    auto base_id = get_base_id(view_name);
    auto& base_cf = _db.find_column_family(base_id);
    auto erm = base_cf.get_effective_replication_map();
    auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(base_id);

    view_tasks v_tasks;
    for (auto tid = std::optional(tablet_map.first_tablet()); tid; tid = tablet_map.next_tablet(*tid)) {
        const auto& tablet_info = tablet_map.get_tablet_info(*tid);
        auto range = tablet_map.get_token_range(*tid);

        data_value start = range.start() ? data_value(range.start()->value().data()) : std::numeric_limits<int64_t>::min();
        data_value end = range.end() ? data_value(range.end()->value().data()) : std::numeric_limits<int64_t>::min();

        for (auto& replica: tablet_info.replicas) {
            std::vector<data_value_or_unset> row {
                data_value(view_name.first), data_value(view_name.second), 
                data_value(replica.host.uuid()), data_value(int32_t(replica.shard)),
                start, end
            };
            auto muts = co_await _sys_ks.query_processor().get_mutations_internal(
                    query, 
                    vb_coordinator_query_state(), 
                    batch.write_timestamp(), 
                    std::move(row));
            batch.add_mutations(std::move(muts));

            view_building_target target{replica.host, replica.shard};
            if (!v_tasks.contains(target)) {
                v_tasks.insert({target, {}});
            }
            v_tasks[target].push_back(range);
        }
    }

    if (!state_copy.tasks.contains(base_id)) {
        state_copy.tasks.insert({base_id, {}});
    }
    state_copy.tasks[base_id].insert({view_name, std::move(v_tasks)});
}

future<> view_building_coordinator::remove_view(const view_name& view_name, vbc_state& state_copy, group0_batch& batch) {
    vbc_logger.info("Unregister all remaining tasks for view: {}.{}", view_name.first, view_name.second);
    static const sstring query = format("DELETE FROM {}.{} WHERE keyspace_name = ? AND view_name = ?", db::system_keyspace::NAME, db::system_keyspace::VIEW_BUILDING_COORDINATOR_TASKS);

    auto mutations = co_await _sys_ks.query_processor().get_mutations_internal(
        query, 
        vb_coordinator_query_state(),
        batch.write_timestamp(),
        {view_name.first, view_name.second});
    
    batch.add_mutations(std::move(mutations));

    auto base_id = get_base_id(view_name);
    state_copy.tasks[base_id].erase(view_name);
    if (state_copy.tasks[base_id].empty()) {
        state_copy.tasks.erase(base_id);
    }
}

future<> run_view_building_coordinator(abort_source& as, replica::database& db, raft_group0& group0, db::system_keyspace& sys_ks, const topology_state_machine& topo_sm) {
    view_building_coordinator vb_coordinator{as, db, group0, sys_ks, topo_sm};

    std::exception_ptr ex;
    db.get_notifier().register_listener(&vb_coordinator);
    try {
        co_await with_scheduling_group(group0.get_scheduling_group(), [&] {
            return vb_coordinator.run();
        });
    } catch (...) {
        ex = std::current_exception();
    }
    if (ex) {
        on_fatal_internal_error(vbc_logger, format("unhandled exception in view_building_coordinator::run(): {}", ex));
    }

    co_await db.get_notifier().unregister_listener(&vb_coordinator);

    co_return;
}

}

}