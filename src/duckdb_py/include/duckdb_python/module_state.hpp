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

// Global lock object for critical sections
// NOTE: This is not multi-interpreter safe - all interpreters share the same lock
// TODO: Replace with proper per-interpreter locking for multi-interpreter support
extern py::object g_global_lock_object;

static py::object GetGlobalLockObject() {
	return g_global_lock_object;
}

// Module state structure to hold per-interpreter state
struct DuckDBPyModuleState {
	// TODO: Make private / move behind a thread-safe accessor
	DefaultConnectionHolder default_connection;

	// Python environment tracking
	PythonEnvironmentType environment = PythonEnvironmentType::NORMAL;
	string formatted_python_version;

	DuckDBPyModuleState();

	template <class T>
	shared_ptr<DuckDB> GetOrCreateInstance(const string &database_path, DBConfig &config, bool cache_instance,
	                                       T &&instantiate_function) {
#ifdef Py_GIL_DISABLED
		py::scoped_critical_section guard(GetGlobalLockObject());
#endif
		return instance_cache->GetOrCreateInstance(database_path, config, cache_instance,
		                                           std::forward<T>(instantiate_function));
	}
	void CloseConnection();

	PythonImportCache *GetImportCache() {
#ifdef Py_GIL_DISABLED
		py::scoped_critical_section guard(GetGlobalLockObject());
#endif
		return import_cache.get();
	}
	void ResetImportCache() {
#ifdef Py_GIL_DISABLED
		py::scoped_critical_section guard(GetGlobalLockObject());
#endif
		import_cache.reset();
	}

private:
	// Thread-sensitive caches - private to enforce thread-safe access
	shared_ptr<PythonImportCache> import_cache;
	std::unique_ptr<DBInstanceCache> instance_cache;

	// Non-copyable
	DuckDBPyModuleState(const DuckDBPyModuleState &) = delete;
	DuckDBPyModuleState &operator=(const DuckDBPyModuleState &) = delete;
};

DuckDBPyModuleState &GetModuleState();

} // namespace duckdb