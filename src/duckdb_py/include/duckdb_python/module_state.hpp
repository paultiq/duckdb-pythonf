//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/module_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb_python/import_cache/python_import_cache.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {

// Module state structure to hold per-interpreter state
struct DuckDBPyModuleState {
	// Core state
	DefaultConnectionHolder default_connection;
	shared_ptr<PythonImportCache> import_cache;
	std::unique_ptr<DBInstanceCache> instance_cache;

	// Python environment tracking
	PythonEnvironmentType environment = PythonEnvironmentType::NORMAL;
	string formatted_python_version;

	DuckDBPyModuleState();

private:
	// Non-copyable
	DuckDBPyModuleState(const DuckDBPyModuleState &) = delete;
	DuckDBPyModuleState &operator=(const DuckDBPyModuleState &) = delete;
};

DuckDBPyModuleState &GetModuleState();

} // namespace duckdb