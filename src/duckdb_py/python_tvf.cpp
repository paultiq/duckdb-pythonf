#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb_python/arrow/arrow_array_stream.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb_python/python_conversion.hpp"
#include "duckdb_python/python_objects.hpp"

namespace duckdb {

// Custom TableFunctionInfo to store Python callable and schema
struct PyTVFInfo : public TableFunctionInfo {
	py::function callable;
	vector<LogicalType> return_types;
	vector<string> return_names;
	PythonTVFType return_type;

	PyTVFInfo(py::function callable_p, vector<LogicalType> types_p, vector<string> names_p, PythonTVFType return_type_p)
	    : callable(std::move(callable_p)), return_types(std::move(types_p)), return_names(std::move(names_p)),
	      return_type(return_type_p) {
	}

	~PyTVFInfo() override {
		// Acquire GIL for entire destructor scope
		py::gil_scoped_acquire acquire;
		// Clear the Python object (GIL is already held)
		callable = py::function();
	}
};

struct PyTVFBindData : public TableFunctionData {
	string func_name;
	vector<Value> args;
	named_parameter_map_t kwargs;
	vector<LogicalType> return_types;
	vector<string> return_names;
	py::function callable;

	PyTVFBindData(string func_name, vector<Value> args, named_parameter_map_t kwargs, vector<LogicalType> return_types,
	              vector<string> return_names, py::function callable)
	    : func_name(std::move(func_name)), args(std::move(args)), kwargs(std::move(kwargs)),
	      return_types(std::move(return_types)), return_names(std::move(return_names)), callable(std::move(callable)) {
	}
};

struct PyTVFTuplesGlobalState : public GlobalTableFunctionState {
	// TUPLES streaming iterator consumption
	Optional<py::object> python_iterator;
	bool iterator_exhausted = false;

	PyTVFTuplesGlobalState() {
		python_iterator = py::none();
		iterator_exhausted = false;
	}

	~PyTVFTuplesGlobalState() {
		py::gil_scoped_acquire gil;
		if (!python_iterator.is_none()) {
			python_iterator = py::none();
		}
	}
};

struct PyTVFArrowGlobalState : public GlobalTableFunctionState {
	unique_ptr<PythonTableArrowArrayStreamFactory> arrow_factory;
	unique_ptr<FunctionData> arrow_bind_data;
	unique_ptr<GlobalTableFunctionState> arrow_global_state;
	Optional<py::object> arrow_result; // Keep Python object alive
	idx_t num_columns;                 // Number of columns in Arrow table

	PyTVFArrowGlobalState() = default;

	~PyTVFArrowGlobalState() {
		py::gil_scoped_acquire gil;
		if (!arrow_result.is_none()) {
			arrow_result = py::none();
		}
	}
};

static void PyTVFTuplesScanFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &gs = input.global_state->Cast<PyTVFTuplesGlobalState>();
	auto &bd = input.bind_data->Cast<PyTVFBindData>();

	if (gs.iterator_exhausted || gs.python_iterator.is_none()) {
		output.SetCardinality(0);
		return;
	}

	py::gil_scoped_acquire gil;
	auto &it = gs.python_iterator;

	idx_t row_idx = 0;
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		py::object next_item;
		try {
			next_item = it.attr("__next__")();
		} catch (py::error_already_set &e) {
			if (e.matches(PyExc_StopIteration)) {
				gs.iterator_exhausted = true;
				PyErr_Clear();
				break;
			}
			throw;
		}

		try {
			// Extract each column from the tuple/list
			for (idx_t col_idx = 0; col_idx < bd.return_types.size(); col_idx++) {
				auto py_val = next_item[py::int_(col_idx)];
				Value duck_val = TransformPythonValue(py_val, bd.return_types[col_idx]);
				output.SetValue(col_idx, row_idx, duck_val);
			}
		} catch (py::error_already_set &e) {
			throw InvalidInputException("Table function '%s' returned invalid data: %s", bd.func_name, e.what());
		}
		row_idx++;
	}
	output.SetCardinality(row_idx);
}

struct PyTVFArrowLocalState : public LocalTableFunctionState {
	unique_ptr<LocalTableFunctionState> arrow_local_state;

	explicit PyTVFArrowLocalState(unique_ptr<LocalTableFunctionState> arrow_local)
	    : arrow_local_state(std::move(arrow_local)) {
	}
};

static void PyTVFArrowScanFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	// Delegates to ArrowScanFunction
	auto &gs = input.global_state->Cast<PyTVFArrowGlobalState>();
	auto &ls = input.local_state->Cast<PyTVFArrowLocalState>();

	TableFunctionInput arrow_input(gs.arrow_bind_data.get(), ls.arrow_local_state.get(), gs.arrow_global_state.get());
	ArrowTableFunction::ArrowScanFunction(context, arrow_input, output);
}

static unique_ptr<PyTVFBindData> PyTVFBindInternal(ClientContext &context, TableFunctionBindInput &in,
                                                   vector<LogicalType> &return_types, vector<string> &return_names) {
	// Disable progress bar to prevent GIL deadlock with Jupyter
	// TODO: Decide if this is still needed - was a problem when fully materializing, but switched to streaming
	ClientConfig::GetConfig(context).enable_progress_bar = false;
	ClientConfig::GetConfig(context).system_progress_bar_disable_reason =
	    "Table Valued Functions do not support the progress bar";

	if (!in.info) {
		throw InvalidInputException("Table function '%s' missing function info", in.table_function.name);
	}

	auto &tvf_info = in.info->Cast<PyTVFInfo>();
	return_types = tvf_info.return_types;
	return_names = tvf_info.return_names;

	return make_uniq<PyTVFBindData>(in.table_function.name, in.inputs, in.named_parameters, return_types, return_names,
	                                tvf_info.callable);
}

static unique_ptr<FunctionData> PyTVFTuplesBindFunction(ClientContext &context, TableFunctionBindInput &in,
                                                        vector<LogicalType> &return_types,
                                                        vector<string> &return_names) {
	auto bd = PyTVFBindInternal(context, in, return_types, return_names);
	return std::move(bd);
}

static unique_ptr<FunctionData> PyTVFArrowBindFunction(ClientContext &context, TableFunctionBindInput &in,
                                                       vector<LogicalType> &return_types,
                                                       vector<string> &return_names) {
	auto bd = PyTVFBindInternal(context, in, return_types, return_names);
	return std::move(bd);
}

static py::object CallPythonTVF(ClientContext &context, const PyTVFBindData &bd) {
	py::gil_scoped_acquire gil;

	// Build positional arguments
	py::tuple args(bd.args.size());
	for (idx_t i = 0; i < bd.args.size(); i++) {
		args[i] = PythonObject::FromValue(bd.args[i], bd.args[i].type(), context.GetClientProperties());
	}

	// Build keyword arguments
	py::dict kwargs;
	for (auto &kv : bd.kwargs) {
		kwargs[py::str(kv.first)] = PythonObject::FromValue(kv.second, kv.second.type(), context.GetClientProperties());
	}

	// Call Python function
	py::object result = bd.callable(*args, **kwargs);

	if (result.is_none()) {
		throw InvalidInputException("Table function '%s' returned None, expected iterable or Arrow table",
		                            bd.func_name);
	}

	return result;
}

static unique_ptr<GlobalTableFunctionState> PyTVFTuplesInitGlobal(ClientContext &context, TableFunctionInitInput &in) {
	auto &bd = in.bind_data->Cast<PyTVFBindData>();
	auto gs = make_uniq<PyTVFTuplesGlobalState>();

	py::object result = CallPythonTVF(context, bd);

	py::gil_scoped_acquire gil;
	try {
		py::iterator it = py::iter(result);
		gs->python_iterator = it;
		gs->iterator_exhausted = false;
	} catch (const py::error_already_set &e) {
		throw InvalidInputException("Table function '%s' returned non-iterable result: %s", bd.func_name, e.what());
	}

	return std::move(gs);
}

static unique_ptr<GlobalTableFunctionState> PyTVFArrowInitGlobal(ClientContext &context, TableFunctionInitInput &in) {
	auto &bd = in.bind_data->Cast<PyTVFBindData>();
	auto gs = make_uniq<PyTVFArrowGlobalState>();

	py::object result = CallPythonTVF(context, bd);
	PyObject *ptr = result.ptr();

	// TODO: Should we verify this is an arrow table, or just fail later
	gs->arrow_result = result;

	gs->arrow_factory =
	    make_uniq<PythonTableArrowArrayStreamFactory>(ptr, context.GetClientProperties(), DBConfig::GetConfig(context));

	// Build bind input for Arrow scan
	vector<Value> children;
	children.push_back(Value::POINTER(CastPointerToValue(gs->arrow_factory.get())));
	children.push_back(Value::POINTER(CastPointerToValue(PythonTableArrowArrayStreamFactory::Produce)));
	children.push_back(Value::POINTER(CastPointerToValue(PythonTableArrowArrayStreamFactory::GetSchema)));

	TableFunctionRef empty_ref;
	duckdb::TableFunction dummy_tf;
	dummy_tf.name = "PyTVFArrowWrapper";

	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr, dummy_tf,
	                                  empty_ref);

	vector<LogicalType> return_types;
	vector<string> return_names;
	gs->arrow_bind_data = ArrowTableFunction::ArrowScanBind(context, bind_input, return_types, return_names);

	gs->num_columns = return_types.size();
	vector<column_t> all_columns;
	for (idx_t i = 0; i < gs->num_columns; i++) {
		all_columns.push_back(i);
	}

	TableFunctionInitInput init_input(gs->arrow_bind_data.get(), all_columns, all_columns, in.filters.get());
	gs->arrow_global_state = ArrowTableFunction::ArrowScanInitGlobal(context, init_input);

	return std::move(gs);
}

static unique_ptr<LocalTableFunctionState> PyTVFArrowInitLocal(ExecutionContext &context, TableFunctionInitInput &in,
                                                               GlobalTableFunctionState *gstate) {
	auto &gs = gstate->Cast<PyTVFArrowGlobalState>();

	vector<column_t> all_columns;
	for (idx_t i = 0; i < gs.num_columns; i++) {
		all_columns.push_back(i);
	}

	TableFunctionInitInput arrow_init(gs.arrow_bind_data.get(), all_columns, all_columns, in.filters.get());
	auto arrow_local_state =
	    ArrowTableFunction::ArrowScanInitLocalInternal(context.client, arrow_init, gs.arrow_global_state.get());

	return make_uniq<PyTVFArrowLocalState>(std::move(arrow_local_state));
}

duckdb::TableFunction DuckDBPyConnection::CreateTableFunctionFromCallable(const std::string &name,
                                                                          const py::function &callable,
                                                                          const py::object &parameters,
                                                                          const py::object &schema,
                                                                          PythonTVFType type) {

	// Schema
	if (schema.is_none()) {
		throw InvalidInputException("Table functions require a schema.");
	}

	vector<LogicalType> types;
	vector<string> names;
	for (auto c : py::iter(schema)) {
		auto item = py::cast<py::object>(c);
		if (py::isinstance<py::str>(item)) {
			throw InvalidInputException("Invalid schema format: expected [name, type] pairs, got string '%s'",
			                            py::str(item).cast<std::string>());
		}
		if (!py::hasattr(item, "__getitem__") || py::len(item) < 2) {
			throw InvalidInputException("Invalid schema format: each schema item must be a [name, type] pair");
		}
		names.emplace_back(py::str(item[py::int_(0)]));
		types.emplace_back(TransformStringToLogicalType(py::str(item[py::int_(1)])));
	}

	if (types.empty()) {
		throw InvalidInputException("Table function '%s' schema cannot be empty", name);
	}

	duckdb::TableFunction tf;
	switch (type) {
	case PythonTVFType::TUPLES:
		tf =
		    duckdb::TableFunction(name, {}, +PyTVFTuplesScanFunction, +PyTVFTuplesBindFunction, +PyTVFTuplesInitGlobal);
		break;
	case PythonTVFType::ARROW_TABLE:
		tf = duckdb::TableFunction(name, {}, +PyTVFArrowScanFunction, +PyTVFArrowBindFunction, +PyTVFArrowInitGlobal,
		                           +PyTVFArrowInitLocal);
		break;
	default:
		throw InvalidInputException("Unknown return type for table function '%s'", name);
	}

	// Store the Python callable and schema in the table function info
	tf.function_info = make_shared_ptr<PyTVFInfo>(callable, types, names, type);

	// args
	tf.varargs = LogicalType::ANY;
	tf.named_parameters["args"] = LogicalType::ANY;

	// kwargs
	if (!parameters.is_none()) {
		for (auto &param : py::cast<py::list>(parameters)) {
			string param_name = py::str(param);
			tf.named_parameters[param_name] = LogicalType::ANY;
		}
	}

	return tf;
}

} // namespace duckdb
