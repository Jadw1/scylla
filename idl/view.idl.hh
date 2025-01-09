/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "dht/i_partitioner_fwd.hh"

namespace db {
namespace view {
class update_backlog {
    size_t get_current_bytes();
    size_t get_max_bytes();
};

<<<<<<< Updated upstream
verb [[cancellable]] build_views_request(std::pair<sstring, sstring> base_table, unsigned shard, dht::token_range range, std::vector<std::pair<sstring, sstring>> views) -> dht::token_range;
=======
verb [[cancellable]] build_views_request(table_id base_id, unsigned shard, dht::token_range range, std::vector<table_id> views);
verb [[one_way]] abort_vbc_work();
>>>>>>> Stashed changes
}
}
