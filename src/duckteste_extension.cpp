#define DUCKDB_EXTENSION_MAIN

#include "duckteste_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void DucktesteScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Duckteste " + name.GetString() + " 🐥");
	});
}

inline void DucktesteBinaryFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &a = args.data[0];
	auto &b = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, string_t>(a, b, result, args.size(), [&](string_t left, string_t right) {
		return StringVector::AddString(result, left.GetString() + " & " + right.GetString());
	});
}

inline void DucktesteOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Duckteste " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

struct SumSquaresState {
    double total;
};

struct SumSquaresOperation {
    using STATE = SumSquaresState;
    using INPUT_TYPE = double;
    using RESULT_TYPE = double;

    static void Initialize(STATE &state) {
        state.total = 0;
    }

    static void Operation(STATE &state, const INPUT_TYPE &input, AggregateInputData &) {
        state.total += input * input;
    }

    static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
        target.total += source.total;
    }

    static void Finalize(const STATE &state, AggregateInputData &, RESULT_TYPE &target) {
        target = state.total;
    }
};


inline void RegisterSumSquares(AggregateFunctionSet &set) {
    auto input_type = LogicalType::DOUBLE;
    auto result_type = LogicalType::DOUBLE;

    auto fun = AggregateFunction::UnaryAggregate<SumSquaresState, double, double, SumSquaresOperation>(
        input_type, result_type
    );

    set.AddFunction(fun);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto duckteste_scalar_function = ScalarFunction("duckteste", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DucktesteScalarFun);
	loader.RegisterFunction(duckteste_scalar_function);

	auto duckteste_binary_function = ScalarFunction("duckteste_binary", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                                 LogicalType::VARCHAR, DucktesteBinaryFun);
	loader.RegisterFunction(duckteste_binary_function);

	// Register another scalar function
	auto duckteste_openssl_version_scalar_function = ScalarFunction("duckteste_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, DucktesteOpenSSLVersionScalarFun);
	loader.RegisterFunction(duckteste_openssl_version_scalar_function);

	AggregateFunctionSet sumsq("sumsq");
    RegisterSumSquares(sumsq);
    loader.RegisterFunction(sumsq);
}

void DucktesteExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DucktesteExtension::Name() {
	return "duckteste";
}

std::string DucktesteExtension::Version() const {
#ifdef EXT_VERSION_DUCKTESTE
	return EXT_VERSION_DUCKTESTE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duckteste, loader) {
	duckdb::LoadInternal(loader);
}
}
