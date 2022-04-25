/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "abstract_function.hh"
#include "cql3/functions/function_name.hh"
#include "scalar_function.hh"
#include "aggregate_function.hh"

namespace cql3 {
namespace functions {

class user_aggregate : public abstract_function, public aggregate_function{
    bytes_opt _initcond;
    ::shared_ptr<scalar_function> _sfunc;
    ::shared_ptr<scalar_function> _reducefunc;
    ::shared_ptr<scalar_function> _finalfunc;
public:
    user_aggregate(function_name fname, bytes_opt initcond, ::shared_ptr<scalar_function> sfunc, ::shared_ptr<scalar_function> reducefunc, ::shared_ptr<scalar_function> finalfunc);
    virtual std::unique_ptr<aggregate_function::aggregate> new_aggregate() override;
    virtual bool is_pure() const override;
    virtual bool is_native() const override;
    virtual bool is_aggregate() const override;
    virtual bool is_reducible() const override;
    virtual bool requires_thread() const override;
    bool has_finalfunc() const;
    ::shared_ptr<user_aggregate> reducible_aggregate() const;

    const scalar_function& sfunc() const {
        return *_sfunc;
    }
    scalar_function& reducefunc() const {
        return *_reducefunc;
    }
    scalar_function& finalfunc() const {
        return *_finalfunc;
    }
    const bytes_opt& initcond() const {
        return _initcond;
    }
};

}
}
