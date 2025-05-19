#pragma once

#include "config.h"

#if USE_LIBPQXX
#include <base/FnTraits.h>
#include <Core/PostgreSQL/ConnectionHolder.h>
#include <Core/NamesAndTypes.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>

namespace DB
{

struct PostgreSQLTableStructure
{
    struct PGAttribute
    {
        Int32 atttypid;
        Int32 atttypmod;
        Int32 attnum;
        bool atthasdef;
        char attgenerated;
        std::string attr_def;
    };
    using Attributes = std::unordered_map<std::string, PGAttribute>;

    struct ColumnsInfo
    {
        NamesAndTypesList columns;
        Attributes attributes;
        std::vector<std::string> names;
        ColumnsInfo(NamesAndTypesList && columns_, Attributes && attributes_) : columns(columns_), attributes(attributes_) {}
    };
    using ColumnsInfoPtr = std::shared_ptr<ColumnsInfo>;

    ColumnsInfoPtr physical_columns;
    ColumnsInfoPtr primary_key_columns;
    ColumnsInfoPtr replica_identity_columns;
};

using PostgreSQLTableStructurePtr = std::unique_ptr<PostgreSQLTableStructure>;

/// We need order for materialized version.
std::set<String> fetchPostgreSQLTablesList(pqxx::connection & connection, const String & postgres_schema);

PostgreSQLTableStructure fetchPostgreSQLTableStructure(
    pqxx::connection & connection, const String & postgres_table, const String & postgres_schema, bool use_nulls = true);

template<typename T>
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
    T & tx, const String & postgres_table, const String & postgres_schema, bool use_nulls = true,
    bool with_primary_key = false, bool with_replica_identity_index = false, const Strings & columns = {});

template<typename T>
std::set<String> fetchPostgreSQLTablesList(T & tx, const String & postgres_schema);

// TODO: Come up with a better place for this function. Need to deal with implicit template (Fn<void()> auto).
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

DataTypePtr convertPostgreSQLDataType(String & type, Fn<void()> auto && recheck_array, bool is_nullable = false, uint16_t dimensions = 0)
{
    DataTypePtr res;
    bool is_array = false;

    /// Get rid of trailing '[]' for arrays
    if (type.ends_with("[]"))
    {
        is_array = true;

        while (type.ends_with("[]"))
            type.resize(type.size() - 2);
    }

    if (type == "smallint")
        res = std::make_shared<DataTypeInt16>();
    else if (type == "integer")
        res = std::make_shared<DataTypeInt32>();
    else if (type == "bigint")
        res = std::make_shared<DataTypeInt64>();
    else if (type == "boolean")
        res = std::make_shared<DataTypeUInt8>();
    else if (type == "real")
        res = std::make_shared<DataTypeFloat32>();
    else if (type == "double precision")
        res = std::make_shared<DataTypeFloat64>();
    else if (type == "serial")
        res = std::make_shared<DataTypeUInt32>();
    else if (type == "bigserial")
        res = std::make_shared<DataTypeUInt64>();
    else if (type.starts_with("timestamp"))
        res = std::make_shared<DataTypeDateTime64>(6);
    else if (type == "date")
        res = std::make_shared<DataTypeDate>();
    else if (type == "uuid")
        res = std::make_shared<DataTypeUUID>();
    else if (type.starts_with("numeric"))
    {
        /// Numeric and decimal will both end up here as numeric. If it has type and precision,
        /// there will be Numeric(x, y), otherwise just Numeric
        UInt32 precision;
        UInt32 scale;
        if (type.ends_with(")"))
        {
            res = DataTypeFactory::instance().get(type);
            precision = getDecimalPrecision(*res);
            scale = getDecimalScale(*res);

            if (precision <= DecimalUtils::max_precision<Decimal32>)
                res = std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
            else if (precision <= DecimalUtils::max_precision<Decimal64>)
                res = std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
            else if (precision <= DecimalUtils::max_precision<Decimal128>)
                res = std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
            else if (precision <= DecimalUtils::max_precision<Decimal256>)
                res = std::make_shared<DataTypeDecimal<Decimal256>>(precision, scale);
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Precision {} and scale {} are too big and not supported", precision, scale);
        }
        else
        {
            precision = DecimalUtils::max_precision<Decimal128>;
            scale = precision / 2;
            res = std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
        }
    }

    if (!res)
        res = std::make_shared<DataTypeString>();
    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);

    if (is_array)
    {
        /// In some cases att_ndims does not return correct number of dimensions
        /// (it might return incorrect 0 number, for example, when a postgres table is created via 'as select * from table_with_arrays').
        /// So recheck all arrays separately afterwards. (Cannot check here on the same connection because another query is in execution).
        if (!dimensions)
        {
            /// Return 1d array type and recheck all arrays dims with array_ndims
            res = std::make_shared<DataTypeArray>(res);
            recheck_array();
        }
        else
        {
            while (dimensions--)
                res = std::make_shared<DataTypeArray>(res);
        }
    }

    return res;
}

}

#endif
