//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/module_state.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb_python/module_state.hpp"
#include <stdexcept>

namespace duckdb {

// TODO: Make non-static. 
// Left static because of scope required to efficiently pass import_cache
// without expensive lookups
static DuckDBPyModuleState* g_module_state;

// Module state constructor
DuckDBPyModuleState::DuckDBPyModuleState() {
	// Create caches
	instance_cache = make_uniq<DBInstanceCache>();
	import_cache = make_shared_ptr<PythonImportCache>();

	// Detects Python environment and version during intialization
	// Moved from DuckDBPyConnection::DetectEnvironment()
	py::module_ sys = py::module_::import("sys");
	py::object version_info = sys.attr("version_info");
	int major = py::cast<int>(version_info.attr("major"));
	int minor = py::cast<int>(version_info.attr("minor"));
	formatted_python_version = std::to_string(major) + "." + std::to_string(minor);

	// If __main__ does not have a __file__ attribute, we are in interactive mode
	auto main_module = py::module_::import("__main__");
	if (!py::hasattr(main_module, "__file__")) {
		environment = PythonEnvironmentType::INTERACTIVE;

		if (ModuleIsLoaded<IpythonCacheItem>()) {
			// Check to see if we are in a Jupyter Notebook
			auto get_ipython = import_cache->IPython.get_ipython();
			if (get_ipython.ptr() != nullptr) {
				auto ipython = get_ipython();
				if (py::hasattr(ipython, "config")) {
					py::dict ipython_config = ipython.attr("config");
					if (ipython_config.contains("IPKernelApp")) {
						environment = PythonEnvironmentType::JUPYTER;
					}
				}
			}
		}
	}

}

DuckDBPyModuleState &GetModuleState() {
	// TODO: Externalize this static cache when adding multi-interpreter support
	// For now, single interpreter assumption allows simple static caching
	if (!g_module_state) {
		throw InternalException("Module state not initialized - call SetModuleState() during module init");
	}
	return *g_module_state;
}

void SetModuleState(DuckDBPyModuleState *state) {
	printf("DEBUG: SetModuleState() called - initializing static cache\n");
	g_module_state = state;
}

shared_ptr<DuckDBPyConnection> DuckDBPyModuleState::GetDefaultConnection() {
	return default_connection.Get();
}

void DuckDBPyModuleState::SetDefaultConnection(shared_ptr<DuckDBPyConnection> connection) {
	default_connection.Set(std::move(connection));
}

void DuckDBPyModuleState::ClearDefaultConnection() {
	default_connection.Set(nullptr);
}

PythonImportCache* DuckDBPyModuleState::GetImportCache() {
	return import_cache.get();
}

void DuckDBPyModuleState::ResetImportCache() {
	import_cache.reset();
}

} // namespace duckdb