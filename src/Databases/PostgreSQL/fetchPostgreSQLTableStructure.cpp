#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>

#if USE_LIBPQXX

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/join.hpp>
#include <Common/quoteString.h>
#include <Core/PostgreSQL/Utils.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


template<typename T>
std::set<String> fetchPostgreSQLTablesList(T & tx, const String & postgres_schema)
{
    Names schemas;
    boost::split(schemas, postgres_schema, [](char c){ return c == ','; });
    for (String & key : schemas)
        boost::trim(key);

    std::set<std::string> tables;
    if (schemas.size() <= 1)
    {
        std::string query = fmt::format(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = {}",
            postgres_schema.empty() ? quoteStringPostgreSQL("public") : quoteStringPostgreSQL(postgres_schema));

        for (auto table_name : tx.template stream<std::string>(query))
            tables.insert(std::get<0>(table_name));

        return tables;
    }

    /// We add schema to table name only in case of multiple schemas for the whole database engine.
    /// Because there is no need to add it if there is only one schema.
    /// If we add schema to table name then table can be accessed only this way: database_name.`schema_name.table_name`
    for (const auto & schema : schemas)
    {
        std::string query = fmt::format(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = {}",
            quoteStringPostgreSQL(schema));

        for (auto table_name : tx.template stream<std::string>(query))
            tables.insert(schema + '.' + std::get<0>(table_name));
    }

    return tables;
}

/// Check if PostgreSQL relation is empty.
/// postgres_table must be already quoted + schema-qualified.
template <typename T>
bool isTableEmpty(T & tx, const String & postgres_table)
{
    auto query = fmt::format("SELECT NOT EXISTS (SELECT * FROM {} LIMIT 1);", postgres_table);
    pqxx::result result{tx.exec(query)};
    return result[0][0].as<bool>();
}

template<typename T>
PostgreSQLTableStructure::ColumnsInfoPtr readNamesAndTypesList(
    T & tx, const String & postgres_table, const String & query, bool use_nulls, bool only_names_and_types)
{
    auto columns = NamesAndTypes();
    PostgreSQLTableStructure::Attributes attributes;

    try
    {
        std::set<size_t> recheck_arrays_indexes;
        {
            auto stream{pqxx::stream_from::query(tx, query)};

            size_t i = 0;
            auto recheck_array = [&]() { recheck_arrays_indexes.insert(i); };

            if (only_names_and_types)
            {
                std::tuple<std::string, std::string> row;
                while (stream >> row)
                {
                    columns.push_back(NameAndTypePair(std::get<0>(row), convertPostgreSQLDataType(std::get<1>(row), recheck_array)));
                    ++i;
                }
            }
            else
            {
                std::tuple<std::string, std::string, std::string, uint16_t, std::string, std::string, std::string, std::string> row;
                while (stream >> row)
                {
                    const auto column_name = std::get<0>(row);
                    const auto data_type = convertPostgreSQLDataType(
                        std::get<1>(row), recheck_array,
                        use_nulls && (std::get<2>(row) == /* not nullable */"f"),
                        std::get<3>(row));

                    columns.push_back(NameAndTypePair(column_name, data_type));
                    auto attgenerated = std::get<7>(row);

                    attributes.emplace(
                        column_name,
                        PostgreSQLTableStructure::PGAttribute{
                            .atttypid = parse<int>(std::get<4>(row)),
                            .atttypmod = parse<int>(std::get<5>(row)),
                            .attnum = parse<int>(std::get<6>(row)),
                            .atthasdef = false,
                            .attgenerated = attgenerated.empty() ? char{} : char(attgenerated[0]),
                            .attr_def = {}
                    });

                    ++i;
                }
            }

            stream.complete();
        }

        for (const auto & i : recheck_arrays_indexes)
        {
            const auto & name_and_type = columns[i];

            /// If the relation is empty, then array_ndims returns NULL.
            /// ClickHouse cannot support this use case.
            if (isTableEmpty(tx, postgres_table))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "PostgreSQL relation containing arrays cannot be empty: {}", postgres_table);

            /// All rows must contain the same number of dimensions.
            /// 1 is ok. If number of dimensions in all rows is not the same -
            /// such arrays are not able to be used as ClickHouse Array at all.
            ///
            /// For empty arrays, array_ndims([]) will return NULL.
            auto postgres_column = doubleQuoteString(name_and_type.name);
            pqxx::result result{tx.exec(
                fmt::format("SELECT {} IS NULL, array_ndims({}) FROM {} LIMIT 1;", postgres_column, postgres_column, postgres_table))};

            /// Nullable(Array) is not supported.
            auto is_null_array = result[0][0].as<bool>();
            if (is_null_array)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "PostgreSQL array cannot be NULL: {}.{}", postgres_table, postgres_column);

            /// Cannot infer dimension of empty arrays.
            auto is_empty_array = result[0][1].is_null();
            if (is_empty_array)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "PostgreSQL cannot infer dimensions of an empty array: {}.{}. Make sure no empty array values in the first row.",
                    postgres_table,
                    postgres_column);
            }

            int dimensions = result[0][1].as<int>();

            /// It is always 1d array if it is in recheck.
            DataTypePtr type = assert_cast<const DataTypeArray *>(name_and_type.type.get())->getNestedType();
            while (dimensions--)
                type = std::make_shared<DataTypeArray>(type);

            columns[i] = NameAndTypePair(name_and_type.name, type);
        }
    }
    catch (const pqxx::undefined_table &)
    {
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "PostgreSQL table {} does not exist", postgres_table);
    }
    catch (const pqxx::syntax_error & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error: {} (in query: {})", e.what(), query);
    }
    catch (Exception & e)
    {
        e.addMessage("while fetching postgresql table structure");
        throw;
    }

    return !columns.empty()
        ? std::make_shared<PostgreSQLTableStructure::ColumnsInfo>(NamesAndTypesList(columns.begin(), columns.end()), std::move(attributes))
        : nullptr;
}


template<typename T>
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
        T & tx, const String & postgres_table, const String & postgres_schema, bool use_nulls, bool with_primary_key, bool with_replica_identity_index, const Strings & columns)
{
    PostgreSQLTableStructure table;

    auto where = fmt::format("relname = {}", quoteStringPostgreSQL(postgres_table));

    where += postgres_schema.empty()
        ? " AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')"
        : fmt::format(" AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = {})", quoteStringPostgreSQL(postgres_schema));

    std::string columns_part;
    if (!columns.empty())
        columns_part = fmt::format(" AND attname IN ('{}')", boost::algorithm::join(columns, "','"));

    /// Bypassing the error of the missing column `attgenerated` in the system table `pg_attribute` for PostgreSQL versions below 12.
    /// This trick involves executing a special query to the DBMS in advance to obtain the correct line with comment /// if column has GENERATED.
    /// The result of the query will be the name of the column `attgenerated` or an empty string declaration for PostgreSQL version 11 and below.
    /// This change does not degrade the function's performance but restores support for older versions and fix ERROR: column "attgenerated" does not exist.
    pqxx::result gen_result{tx.exec("select case when current_setting('server_version_num')::int < 120000 then '''''' else 'attgenerated' end as generated")};
    std::string generated = gen_result[0][0].as<std::string>();

    std::string query = fmt::format(
           "SELECT attname AS name, " /// column name
           "format_type(atttypid, atttypmod) AS type, " /// data type
           "attnotnull AS not_null, " /// is nullable
           "attndims AS dims, " /// array dimensions
           "atttypid as type_id, "
           "atttypmod as type_modifier, "
           "attnum as att_num, "
           "{} as generated " /// if column has GENERATED
           "FROM pg_attribute "
           "WHERE attrelid = (SELECT oid FROM pg_class WHERE {}) {}"
           "AND NOT attisdropped AND attnum > 0 "
           "ORDER BY attnum ASC", generated, where, columns_part); /// Now we use variable `generated` to form query string. End of trick.

    auto postgres_table_with_schema = postgres_schema.empty() ? postgres_table : doubleQuoteString(postgres_schema) + '.' + doubleQuoteString(postgres_table);
    table.physical_columns = readNamesAndTypesList(tx, postgres_table_with_schema, query, use_nulls, false);

    if (!table.physical_columns)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "PostgreSQL table {} does not exist", postgres_table_with_schema);

    for (const auto & column : table.physical_columns->columns)
    {
        table.physical_columns->names.push_back(column.name);
    }

    bool check_generated = table.physical_columns->attributes.end() != std::find_if(
        table.physical_columns->attributes.begin(),
        table.physical_columns->attributes.end(),
        [](const auto & attr){ return attr.second.attgenerated == 's'; });

    if (check_generated)
    {
        std::string attrdef_query = fmt::format(
            "SELECT adnum, pg_get_expr(adbin, adrelid) as generated_expression "
            "FROM pg_attrdef "
            "WHERE adrelid = (SELECT oid FROM pg_class WHERE {});", where);

        pqxx::result result{tx.exec(attrdef_query)};
        if (static_cast<uint64_t>(result.size()) > table.physical_columns->names.size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Received {} attrdef, but currently fetched columns list has {} columns",
                            result.size(), table.physical_columns->attributes.size());
        }

        for (const auto & column_attrs : table.physical_columns->attributes)
        {
            if (column_attrs.second.attgenerated != 's') /// e.g. not a generated column
            {
                continue;
            }

            for (const auto row : result)
            {
                int adnum = row[0].as<int>();
                if (column_attrs.second.attnum == adnum)
                {
                    table.physical_columns->attributes.at(column_attrs.first).attr_def = row[1].as<std::string>();
                    break;
                }
            }
        }
    }

    if (with_primary_key)
    {
        /// wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        query = fmt::format(
                "SELECT a.attname, " /// column name
                "format_type(a.atttypid, a.atttypmod) AS data_type " /// data type
                "FROM pg_index i "
                "JOIN pg_attribute a ON a.attrelid = i.indrelid "
                "AND a.attnum = ANY(i.indkey) "
                "WHERE attrelid = (SELECT oid FROM pg_class WHERE {}) AND i.indisprimary", where);

        table.primary_key_columns = readNamesAndTypesList(tx, postgres_table_with_schema, query, use_nulls, true);
    }

    if (with_replica_identity_index && !table.primary_key_columns)
    {
        query = fmt::format(
            "SELECT "
            "a.attname AS column_name, " /// column name
            "format_type(a.atttypid, a.atttypmod) as type " /// column type
            "FROM "
            "pg_class t, "
            "pg_class i, "
            "pg_index ix, "
            "pg_attribute a "
            "WHERE "
            "t.oid = ix.indrelid "
            "and i.oid = ix.indexrelid "
            "and a.attrelid = t.oid "
            "and a.attnum = ANY(ix.indkey) "
            "and t.relkind in ('r', 'p') " /// simple tables
            "and t.relname = {} " /// Connection is already done to a needed database, only table name is needed.
            "and t.relnamespace = (select oid from pg_namespace where nspname = {}) "
            "and ix.indisreplident = 't' " /// index is is replica identity index
            "ORDER BY a.attname", /// column name
            quoteStringPostgreSQL(postgres_table),
            (postgres_schema.empty() ? quoteStringPostgreSQL("public") : quoteStringPostgreSQL(postgres_schema))
        );

        table.replica_identity_columns = readNamesAndTypesList(tx, postgres_table_with_schema, query, use_nulls, true);
    }

    return table;
}


PostgreSQLTableStructure fetchPostgreSQLTableStructure(pqxx::connection & connection, const String & postgres_table, const String & postgres_schema, bool use_nulls)
{
    pqxx::ReadTransaction tx(connection);
    auto result = fetchPostgreSQLTableStructure(tx, postgres_table, postgres_schema, use_nulls, false, false, {});
    tx.commit();
    return result;
}


std::set<String> fetchPostgreSQLTablesList(pqxx::connection & connection, const String & postgres_schema)
{
    pqxx::ReadTransaction tx(connection);
    auto result = fetchPostgreSQLTablesList(tx, postgres_schema);
    tx.commit();
    return result;
}


template
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
        pqxx::ReadTransaction & tx, const String & postgres_table, const String & postgres_schema,
        bool use_nulls, bool with_primary_key, bool with_replica_identity_index, const Strings & columns);

template
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
        pqxx::ReplicationTransaction & tx, const String & postgres_table, const String & postgres_schema,
        bool use_nulls, bool with_primary_key, bool with_replica_identity_index, const Strings & columns);

template
PostgreSQLTableStructure fetchPostgreSQLTableStructure(
        pqxx::nontransaction & tx, const String & postgres_table, const String & postrges_schema,
        bool use_nulls, bool with_primary_key, bool with_replica_identity_index, const Strings & columns);

std::set<String> fetchPostgreSQLTablesList(pqxx::work & tx, const String & postgres_schema);

template
std::set<String> fetchPostgreSQLTablesList(pqxx::ReadTransaction & tx, const String & postgres_schema);

template
std::set<String> fetchPostgreSQLTablesList(pqxx::nontransaction & tx, const String & postgres_schema);

}

#endif
