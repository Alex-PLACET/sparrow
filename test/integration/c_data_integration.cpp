// Copyright 2024 Man Group Operations Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or mplied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "c_data_integration.hpp"

#include <cstdint>
#include <fstream>
#include <string>

#include <nlohmann/json.hpp>

#include "sparrow/array.hpp"
#include "sparrow/layout/fixed_width_binary_layout/fixed_width_binary_array.hpp"
#include "sparrow/layout/null_array.hpp"
#include "sparrow/layout/primitive_array.hpp"
#include "sparrow/layout/run_end_encoded_layout/run_end_encoded_array.hpp"
#include "sparrow/layout/struct_layout/struct_array.hpp"
#include "sparrow/layout/temporal/date_array.hpp"
#include "sparrow/layout/temporal/duration_array.hpp"
#include "sparrow/layout/temporal/interval_array.hpp"
#include "sparrow/layout/temporal/timestamp_array.hpp"
#include "sparrow/layout/union_array.hpp"
#include "sparrow/layout/variable_size_binary_view_array.hpp"
#include "sparrow/record_batch.hpp"
#include "sparrow/utils/contracts.hpp"


sparrow::array build_array_from_json(const nlohmann::json& array, const nlohmann::json& schema);

static constexpr std::string_view VALIDITY = "VALIDITY";
static constexpr std::string_view DATA = "DATA";

std::vector<std::pair<const nlohmann::json&, const nlohmann::json&>>
get_children(const nlohmann::json& array, const nlohmann::json& schema)
{
    std::vector<std::pair<const nlohmann::json&, const nlohmann::json&>> children;
    const auto names = schema.at("children")
                       | std::views::transform(
                           [](const nlohmann::json& child)
                           {
                               return child.at("name").get<std::string>();
                           }
                       );
    for (const auto& name : names)
    {
        const auto child_array = array.at("children").at(name);
        const auto child_schema = schema.at("children").at(name);
        children.emplace_back(child_array, child_schema);
    }
    return children;
}

auto get_children_arrays(const nlohmann::json& array, const nlohmann::json& schema)
{
    const auto children_json = get_children(array, schema);
    return children_json
           | std::views::transform(
               [](const auto& [child_array, child_schema])
               {
                   return build_array_from_json(child_array, child_schema);
               }
           );
}

void check_type(const nlohmann::json& array, const nlohmann::json& schema, const std::string& type)
{
    const std::string schema_type = schema.at("type").at("name").get<std::string>();
    if (schema_type != type)
    {
        throw std::runtime_error("Invalid type");
    }
}

sparrow::array null_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "null");
    const std::string name = schema.at("name").get<std::string>();
    const std::size_t count = array.at("count").get<std::size_t>();
    sparrow::null_array ar{count, name};
    return sparrow::array{std::move(ar)};
}

sparrow::array struct_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "struct");
    const std::string name = schema.at("name").get<std::string>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();

    sparrow::struct_array ar{
        get_children_arrays(array, schema),
        std::move(validity),
        name,
    };
    return sparrow::array{std::move(ar)};
}

sparrow::array list_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "list");
    const std::string name = schema.at("name").get<std::string>();
    const auto children_json = get_children(array, schema);
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();
    sparrow::list_array ar{get_children_arrays(array, schema)[0], std::move(validity), name};
    return sparrow::array{std::move(ar)};
}

sparrow::array large_list_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "largelist");
    const std::string name = schema.at("name").get<std::string>();
    const auto children_json = get_children(array, schema);
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();
    sparrow::big_list_array ar{get_children_arrays(array, schema)[0], std::move(validity), name};
    return sparrow::array{std::move(ar)};
}

sparrow::array list_view_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "listview");
    const std::string name = schema.at("name").get<std::string>();
    const auto children_json = get_children(array, schema);
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();
    sparrow::list_view_array ar{get_children_arrays(array, schema)[0], std::move(validity), name};
    return sparrow::array{std::move(ar)};
}

sparrow::array large_list_view_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "largelistview");
    const std::string name = schema.at("name").get<std::string>();
    const auto children_json = get_children(array, schema);
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();
    sparrow::big_list_view_array ar{get_children_arrays(array, schema)[0], std::move(validity), name};
    return sparrow::array{std::move(ar)};
}

sparrow::array primitive_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "int");
    const uint8_t bit_width = schema.at("type").at("bitWidth").get<uint8_t>();
    const bool is_signed = schema.at("type").at("isSigned").get<bool>();
    const std::string name = schema.at("name").get<std::string>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();

    if (is_signed)
    {
        switch (bit_width)
        {
            case 8:
                return sparrow::array{sparrow::primitive_array<int8_t>{
                    array.at(DATA).get<std::vector<int8_t>>(),
                    std::move(validity),
                    name
                }};
            case 16:
                return sparrow::array{sparrow::primitive_array<int16_t>{
                    array.at(DATA).get<std::vector<int16_t>>(),
                    std::move(validity),
                    name
                }};
            case 32:
                return sparrow::array{sparrow::primitive_array<int32_t>{
                    array.at(DATA).get<std::vector<int32_t>>(),
                    std::move(validity),
                    name
                }};
            case 64:
                return sparrow::array{sparrow::primitive_array<int64_t>{
                    array.at(DATA).get<std::vector<int64_t>>(),
                    std::move(validity),
                    name
                }};
        }
    }
    else
    {
        switch (bit_width)
        {
            case 8:
                return sparrow::array{sparrow::primitive_array<uint8_t>{
                    array.at(DATA).get<std::vector<uint8_t>>(),
                    std::move(validity),
                    name
                }};
            case 16:
                return sparrow::array{sparrow::primitive_array<uint16_t>{
                    array.at(DATA).get<std::vector<uint16_t>>(),
                    std::move(validity),
                    name
                }};
            case 32:
                return sparrow::array{sparrow::primitive_array<uint32_t>{
                    array.at(DATA).get<std::vector<uint32_t>>(),
                    std::move(validity),
                    name
                }};
            case 64:
                return sparrow::array{sparrow::primitive_array<uint64_t>{
                    array.at(DATA).get<std::vector<uint64_t>>(),
                    std::move(validity),
                    name
                }};
        }
    }
    throw std::runtime_error("Invalid bit width or signedness");
}

sparrow::array floating_point_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "floatingpoint");
    const std::string precision = schema.at("type").at("precision").get<std::string>();
    const std::string name = schema.at("name").get<std::string>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();

    if (precision == "HALF")
    {
        return sparrow::array{sparrow::primitive_array<sparrow::float16_t>{
            array.at(DATA).get<std::vector<float>>(),
            std::move(validity),
            name
        }};
    }
    else if (precision == "SINGLE")
    {
        return sparrow::array{sparrow::primitive_array<sparrow::float32_t>{
            array.at(DATA).get<std::vector<sparrow::float32_t>>(),
            std::move(validity),
            name
        }};
    }
    else if (precision == "DOUBLE")
    {
        return sparrow::array{sparrow::primitive_array<sparrow::float64_t>{
            array.at(DATA).get<std::vector<sparrow::float64_t>>(),
            std::move(validity),
            name
        }};
    }

    throw std::runtime_error("Invalid precision");
}

// sparrow::int128_t string_to_int128(const std::string& str)
// {
//     if (str.empty())
//     {
//         throw std::invalid_argument("Empty string");
//     }

//     // Handle negative numbers
//     bool negative = str[0] == '-';
//     size_t start_idx = negative ? 1 : 0;

//     if (start_idx >= str.length())
//     {
//         throw std::invalid_argument("Invalid number format");
//     }

//     sparrow::int128_t result = 0;
//     for (size_t i = start_idx; i < str.length(); ++i)
//     {
//         if (!std::isdigit(str[i]))
//         {
//             throw std::invalid_argument("Invalid character in string");
//         }

//         // Check for overflow before multiplying
//         if (result > (sparrow::int128_t)((std::numeric_limits<sparrow::int128_t>::max)() / 10))
//         {
//             throw std::overflow_error("Number too large for int128_t");
//         }

//         result = result * 10 + (str[i] - '0');

//         // Check for overflow after adding
//         if (result < 0)
//         {
//             throw std::overflow_error("Number too large for int128_t");
//         }
//     }

//     return negative ? -result : result;
// }

sparrow::array decimal_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "decimal");
    throw std::runtime_error("Not implemented");
    // const uint32_t precision = schema.at("type").at("precision").get<uint32_t>();
    // const uint32_t scale = schema.at("type").at("scale").get<uint32_t>();
    // const std::string name = schema.at("name").get<std::string>();

    // const std::vector<sparrow::int256_t> data = array.at(DATA).get<std::vector<sparrow::int128_t>>();

    // if (precision == 32)
    // {
    //     return sparrow::array
    //     {
    //         sparrow::decimal_32_array
    //         {
    //             array.at(DATA).get<std::vector<sparrow::decimal32>>(), precision, scale, name
    //         }
    //     }
    // }
};

sparrow::array fixedsizebinary_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "fixedsizebinary");
    const std::string name = schema.at("name").get<std::string>();
    const std::size_t byte_width = schema.at("type").at("byteWidth").get<std::size_t>();
    auto data = array.at(DATA).get<std::vector<std::vector<uint8_t>>>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();
    return sparrow::array{sparrow::fixed_width_binary_array{std::move(data), std::move(validity), name}};
}

sparrow::array string_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "utf8");
    const std::string name = schema.at("name").get<std::string>();
    auto data = array.at(DATA).get<std::vector<std::string>>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();
    return sparrow::array{sparrow::string_array{std::move(data), std::move(validity), name}};
}

sparrow::array big_string_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "largeutf8");
    const std::string name = schema.at("name").get<std::string>();
    auto data = array.at(DATA).get<std::vector<std::string>>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();
    return sparrow::array{sparrow::big_string_array{std::move(data), std::move(validity), name}};
}

sparrow::array string_view_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "utf8_view");
    const std::string name = schema.at("name").get<std::string>();
    auto data = array.at(DATA).get<std::vector<std::string>>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();
    return sparrow::array{sparrow::string_view_array{std::move(data), std::move(validity), name}};
}

sparrow::array date_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "date");
    const std::string name = schema.at("name").get<std::string>();
    const std::string unit = schema.at("type").at("unit").get<std::string>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();

    if (unit == "DAY")
    {
        auto data = array.at(DATA).get<std::vector<int32_t>>();
        return sparrow::array{sparrow::date_days_array{std::move(data), std::move(validity), name}};
    }
    else if (unit == "MILLISECOND")
    {
        auto data = array.at(DATA).get<std::vector<int64_t>>();
        return sparrow::array{sparrow::date_milliseconds_array{std::move(data), std::move(validity), name}};
    }
    else
    {
        throw std::runtime_error("Invalid unit");
    }
}

sparrow::array time_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "time");
    const std::string name = schema.at("name").get<std::string>();
    const std::string unit = schema.at("type").at("unit").get<std::string>();
    auto data = array.at(DATA).get<std::vector<int64_t>>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();

    if (unit == "SECOND")
    {
        return sparrow::array{sparrow::time_seconds_array{std::move(data), std::move(validity), name}};
    }
    else if (unit == "MILLISECOND")
    {
        return sparrow::array{sparrow::time_milliseconds_array{std::move(data), std::move(validity), name}};
    }
    else if (unit == "MICROSECOND")
    {
        return sparrow::array{sparrow::time_microseconds_array{std::move(data), std::move(validity), name}};
    }
    else if (unit == "NANOSECOND")
    {
        return sparrow::array{sparrow::time_nanoseconds_array{std::move(data), std::move(validity), name}};
    }
    else
    {
        throw std::runtime_error("Invalid unit");
    }
}

sparrow::array timestamp_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "timestamp");
    const std::string name = schema.at("name").get<std::string>();
    const std::string unit = schema.at("type").at("unit").get<std::string>();
    std::optional<std::string> timezone;
    if (schema.at("type").contains("timezone"))
    {
        timezone = schema.at("type").at("timezone").get<std::string>();
    }
    const date::time_zone* tz = timezone ? date::locate_zone(*timezone) : nullptr;
    auto data = array.at(DATA).get<std::vector<int64_t>>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();
    if (unit == "SECOND")
    {
        return sparrow::array{sparrow::timestamp_seconds_array{tz, std::move(data), std::move(validity), name}};
    }
    else if (unit == "MILLISECOND")
    {
        return sparrow::array{
            sparrow::timestamp_milliseconds_array{tz, std::move(data), std::move(validity), name}
        };
    }
    else if (unit == "MICROSECOND")
    {
        return sparrow::array{
            sparrow::timestamp_microseconds_array{tz, std::move(data), std::move(validity), name}
        };
    }
    else if (unit == "NANOSECOND")
    {
        return sparrow::array{
            sparrow::timestamp_nanoseconds_array{tz, std::move(data), std::move(validity), name}
        };
    }
    else
    {
        throw std::runtime_error("Invalid unit");
    }
}

sparrow::array duration_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "duration");
    const std::string name = schema.at("name").get<std::string>();
    const std::string unit = schema.at("type").at("unit").get<std::string>();
    auto data = array.at(DATA).get<std::vector<int64_t>>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();
    if (unit == "SECOND")
    {
        return sparrow::array{sparrow::duration_seconds_array{data, std::move(validity), std::move(name)}};
    }
    else if (unit == "MILLISECOND")
    {
        return sparrow::array{sparrow::duration_milliseconds_array{data, std::move(validity), std::move(name)}};
    }
    else if (unit == "MICROSECOND")
    {
        return sparrow::array{sparrow::duration_microseconds_array{data, std::move(validity), std::move(name)}};
    }
    else if (unit == "NANOSECOND")
    {
        return sparrow::array{sparrow::duration_nanoseconds_array{data, std::move(validity), std::move(name)}};
    }
    else
    {
        throw std::runtime_error("Invalid unit");
    }
}

sparrow::array interval_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "interval");
    const std::string name = schema.at("name").get<std::string>();
    const std::string unit = schema.at("type").at("unit").get<std::string>();
    auto data = array.at(DATA).get<std::vector<int64_t>>();
    auto validity = array.at(VALIDITY).get<std::vector<bool>>();
    if (unit == "YEAR_MONTH")
    {
        return sparrow::array{sparrow::months_interval_array{data, std::move(validity), std::move(name)}};
    }
    else if (unit == "DAY_TIME")
    {
        return sparrow::array{sparrow::days_time_interval_array{data, std::move(validity), std::move(name)}};
    }
    else
    {
        throw std::runtime_error("Invalid unit");
    }
}

sparrow::array sparse_union_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "union");
    const std::string mode = schema.at("type").at("mode").get<std::string>();
    if (mode != "sparse")
    {
        throw std::runtime_error("Invalid mode");
    }
    const std::string name = schema.at("name").get<std::string>();
    auto type_ids_values = schema.at("type").at("typeIds").get<std::vector<uint8_t>>();
    const sparrow::sparse_union_array::type_id_buffer_type type_ids{std::move(type_ids_values)};
    const auto children = schema.at("children").get<std::vector<nlohmann::json>>();
    std::vector<sparrow::array> arrays;
    arrays.reserve(children.size());
    for (const auto& child : children)
    {
        arrays.emplace_back(build_array_from_json(array, child));
    }
    return sparrow::array{sparrow::sparse_union_array{std::move(arrays), std::vector<std::uint8_t>{}}};
}

sparrow::array dense_union_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    throw std::runtime_error("Not implemented");
}

sparrow::array union_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "union");
    const std::string mode = schema.at("type").at("mode").get<std::string>();
    if (mode == "dense")
    {
        return dense_union_array_from_json(array, schema);
    }
    else if (mode == "sparse")
    {
        return sparse_union_array_from_json(array, schema);
    }
    else
    {
        throw std::runtime_error("Invalid mode");
    }
}

sparrow::array runendencoded_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    check_type(array, schema, "runencoded");
    return sparrow::array{sparrow::run_end_encoded_array{}};
}

void read_schema_from_json(const nlohmann::json& data)
{
    SPARROW_ASSERT_TRUE(data.is_object());
    const auto fields_it = data.find("fields");
    if (fields_it != data.end())
    {
        SPARROW_ASSERT_TRUE(fields_it->is_array());
        for (const auto& field : *fields_it)
        {
            SPARROW_ASSERT_TRUE(field.is_object());

            const std::string name = field.at("name").get<std::string>();
            const bool nullable = field.at("nullable").get<bool>();
            const auto type = field.at("type");

            const auto dictionary_it = field.find("dictionary");
            if (dictionary_it != field.end())
            {
                SPARROW_ASSERT_TRUE(dictionary_it->is_object());
                const auto id_it = field.find("type");
            }

            const auto children_it = field.find("children");
            if (children_it != field.end())
            {
                SPARROW_ASSERT_TRUE(children_it->is_array());
                read_schema_from_json(*children_it);
            }
        }
    }
}

// Unordered map witk key = type name and value = function
using array_builder_function = std::function<sparrow::array(const nlohmann::json&, const nlohmann::json&)>;
const std::unordered_map<std::string, array_builder_function> array_builders{
    {"null", null_array_from_json},
    {"struct", struct_array_from_json},
    {"list", list_array_from_json},
    {"largelist", large_list_array_from_json},
    {"listview", list_view_array_from_json},
    {"largelistview", large_list_view_array_from_json},
    {"union", union_array_from_json},
    {"int", primitive_array_from_json},
    {"floatingpoint", floating_point_from_json},
    {"utf8", string_array_from_json},
    {"largeutf8", big_string_array_from_json},
    // {"binary", binary_array_from_json},
    // {"largebinary", binary_array_from_json},
    {"utf8view", string_view_from_json},
    // {"binaryview", binary_array_from_json},
    {"fixedsizebinary", fixedsizebinary_from_json},
    // {"bool", primitive_array_from_json},
    {"decimal", decimal_from_json},
    {"date", date_array_from_json},
    {"time", time_array_from_json},
    {"timestamp", timestamp_array_from_json},
    {"interval", interval_array_from_json},
    {"duration", duration_array_from_json},
    {"sparse_union", sparse_union_array_from_json},
    {"runencoded", runendencoded_array_from_json},
};

sparrow::array build_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    const std::string type = schema.at("type").at("name").get<std::string>();
    const auto builder_it = array_builders.find(type);
    if (builder_it == array_builders.end())
    {
        throw std::runtime_error("Invalid type");
    }
    return builder_it->second(array, schema);
}

std::vector<sparrow::record_batch> build_arrays_from_json(const nlohmann::json& data)
{
    const auto schemas = data.at("schema").at("fields");
    std::unordered_map<std::string, nlohmann::json> schema_map;
    for (const auto& schema : schemas)
    {
        const std::string name = schema.at("name").get<std::string>();
        schema_map[name] = schema;
    }

    const auto batches = data.at("batches");
    std::vector<sparrow::record_batch> record_batches;
    record_batches.reserve(batches.size());
    for (const auto& batch : batches)
    {
        const auto columns = batch.at("columns");
        std::vector<sparrow::array> arrays;
        arrays.reserve(columns.size());
        for (const auto& column : columns)
        {
            const auto column_name = column.at("name").get<std::string>();
            const auto& schema = schema_map.at(column_name);
            arrays.emplace_back(build_array_from_json(column, schema));
        }
    }
    return record_batches;
}

const char* nanoarrow_CDataIntegration_ExportSchemaFromJson(const char* json_path, ArrowSchema* out)
{
    std::ifstream json_file(json_path);
    const nlohmann::json data = nlohmann::json::parse(json_file);

    return "coucou";
}
