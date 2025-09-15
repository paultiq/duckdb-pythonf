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
	// Acquire GIL for free-threading safety
	py::gil_scoped_acquire gil;

	auto duckdb_module = py::module_::import("_duckdb");
	try {
		auto capsule = duckdb_module.attr("__duckdb_state").cast<py::capsule>();
		return *static_cast<DuckDBPyModuleState *>(capsule.get_pointer());
	} catch (const py::attribute_error &) {
		throw InternalException("Module state not initialized - __duckdb_state attribute missing");
	}
}

} // namespace duckdb