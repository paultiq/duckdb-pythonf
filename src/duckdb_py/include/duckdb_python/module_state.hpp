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
#include "duckdb/main/database.hpp"
#include "duckdb_python/import_cache/python_import_cache.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include <pybind11/critical_section.h>

namespace duckdb {

// Module state structure to hold per-interpreter state
struct DuckDBPyModuleState {
	// TODO: Make private / move behind a thread-safe accessor
	shared_ptr<DuckDBPyConnection> default_connection_ptr;

	// Python environment tracking
	PythonEnvironmentType environment = PythonEnvironmentType::NORMAL;
	string formatted_python_version;

	DuckDBPyModuleState();

	shared_ptr<DuckDBPyConnection> GetDefaultConnection();
	void SetDefaultConnection(shared_ptr<DuckDBPyConnection> connection);
	void ClearDefaultConnection();

	PythonImportCache *GetImportCache();
	void ClearImportCache();

	DBInstanceCache *GetInstanceCache();

	static DuckDBPyModuleState &GetGlobalModuleState();
	static void SetGlobalModuleState(DuckDBPyModuleState *state);

private:
	PythonImportCache import_cache;
	std::unique_ptr<DBInstanceCache> instance_cache;
#ifdef Py_GIL_DISABLED
	py::object default_con_lock;
#endif

	    // Static module state cache for performance optimization
	    // TODO: Replace with proper per-interpreter state for multi-interpreter support
	    static DuckDBPyModuleState *g_module_state;

	// Non-copyable
	DuckDBPyModuleState(const DuckDBPyModuleState &) = delete;
	DuckDBPyModuleState &operator=(const DuckDBPyModuleState &) = delete;
};

DuckDBPyModuleState &GetModuleState();
void SetModuleState(DuckDBPyModuleState *state);

} // namespace duckdb