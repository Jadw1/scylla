namespace query {
struct forward_request {
    struct count {};
    struct uda {
        sstring keyspace;
        sstring uda_name;
        std::vector<sstring> column_names;
    };
    std::vector<std::variant<query::forward_request::count, query::forward_request::uda>> reductions;

    query::read_command cmd;
    dht::partition_range_vector pr;

    db::consistency_level cl;
    lowres_clock::time_point timeout;
};

struct forward_result {
    std::vector<bytes_opt> query_results;
};

verb forward_request(query::forward_request, std::optional<tracing::trace_info>) -> query::forward_result;
}
