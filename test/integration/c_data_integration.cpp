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
#include "sparrow/layout/temporal/duration_array.hpp"
#include "sparrow/layout/temporal/interval_array.hpp"
#include "sparrow/layout/temporal/timestamp_array.hpp"
#include "sparrow/record_batch.hpp"
#include "sparrow/utils/contracts.hpp"

void read_schema_field(const nlohmann::json& data)
{
    SPARROW_ASSERT_TRUE(data.is_object());
    const std::string name = data.at("name").get<std::string>();
    const bool nullable = data.at("nullable").get<bool>();
    const auto type = data.at("type");
}

sparrow::array build_null_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    // std::optional<std::string> metadata = schema.at("metadata").get<std::optional<std::string>>();
    sparrow::null_array ar{array.at("count").get<std::size_t>(), array.at("name")};
    return sparrow::array{std::move(ar)};
}

static constexpr std::string_view VALIDITY = "VALIDITY";
static constexpr std::string_view DATA = "DATA";

sparrow::array build_struct_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    std::vector<sparrow::array> children;
    children.reserve(schema.at("children").size());
    // TODO

    std::vector<bool> validity;
    validity.reserve(array.at("count").get<std::size_t>());
    array.at(VALIDITY).get_to(validity);

    const std::string name = schema.at("name").get<std::string>();

    sparrow::struct_array ar{
        std::move(children),
        std::move(validity),
        name,
    };
    return sparrow::array{std::move(ar)};
}

// sparrow::array runendencoded_from_json(const nlohmann::json& array, const nlohmann::json& schema)
// {
//     sparrow::run_end_encoded_array ar{}

// }

sparrow::array int_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    const std::string type = schema.at("type").at("name").get<std::string>();
    if (type != "int")
    {
        throw std::runtime_error("Invalid type");
    }
    const uint8_t bit_width = schema.at("type").at("bitWidth").get<uint8_t>();
    const bool is_signed = schema.at("type").at("isSigned").get<bool>();
    const std::string name = schema.at("name").get<std::string>();
    const std::vector<bool> validity = array.at(VALIDITY).get<std::vector<bool>>();

    if (is_signed)
    {
        switch (bit_width)
        {
            case 8:
                return sparrow::array{
                    sparrow::primitive_array<int8_t>{array.at(DATA).get<std::vector<int8_t>>(), validity, name}
                };
            case 16:
                return sparrow::array{
                    sparrow::primitive_array<int16_t>{array.at(DATA).get<std::vector<int16_t>>(), validity, name}
                };
            case 32:
                return sparrow::array{
                    sparrow::primitive_array<int32_t>{array.at(DATA).get<std::vector<int32_t>>(), validity, name}
                };
            case 64:
                return sparrow::array{
                    sparrow::primitive_array<int64_t>{array.at(DATA).get<std::vector<int64_t>>(), validity, name}
                };
        }
    }
    else
    {
        switch (bit_width)
        {
            case 8:
                return sparrow::array{
                    sparrow::primitive_array<uint8_t>{array.at(DATA).get<std::vector<uint8_t>>(), validity, name}
                };
            case 16:
                return sparrow::array{
                    sparrow::primitive_array<uint16_t>{array.at(DATA).get<std::vector<uint16_t>>(), validity, name}
                };
            case 32:
                return sparrow::array{
                    sparrow::primitive_array<uint32_t>{array.at(DATA).get<std::vector<uint32_t>>(), validity, name}
                };
            case 64:
                return sparrow::array{
                    sparrow::primitive_array<uint64_t>{array.at(DATA).get<std::vector<uint64_t>>(), validity, name}
                };
        }
    }
    throw std::runtime_error("Invalid bit width or signedness");
}

sparrow::array floating_point_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    const std::string type = schema.at("type").at("name").get<std::string>();
    if (type != "floatingpoint")
    {
        throw std::runtime_error("Invalid type");
    }
    const std::string precision = schema.at("type").at("precision").get<std::string>();
    const std::string name = schema.at("name").get<std::string>();
    const std::vector<bool> validity = array.at(VALIDITY).get<std::vector<bool>>();

    if (precision == "HALF")
    {
        return sparrow::array{
            sparrow::primitive_array<sparrow::float16_t>{array.at(DATA).get<std::vector<float>>(), validity, name}
        };
    }
    else if (precision == "SINGLE")
    {
        return sparrow::array{sparrow::primitive_array<sparrow::float32_t>{
            array.at(DATA).get<std::vector<sparrow::float32_t>>(),
            validity,
            name
        }};
    }
    else if (precision == "DOUBLE")
    {
        return sparrow::array{sparrow::primitive_array<sparrow::float64_t>{
            array.at(DATA).get<std::vector<sparrow::float64_t>>(),
            validity,
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
    const uint32_t precision = schema.at("type").at("precision").get<uint32_t>();
    const uint32_t scale = schema.at("type").at("scale").get<uint32_t>();
    const std::string name = schema.at("name").get<std::string>();

    const std::vector<sparrow::int256_t> data = array.at(DATA).get<std::vector<sparrow::int128_t>>();

    if (precision == 32)
    {
        return sparrow::array
        {
            sparrow::decimal_32_array
            {
                array.at(DATA).get<std::vector<sparrow::decimal32>>(), precision, scale, name
            }
        }
    }
};

sparrow::array fixedsizebinary_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    const std::string type = schema.at("type").at("name").get<std::string>();
    if (type != "fixedsizebinary")
    {
        throw std::runtime_error("Invalid type");
    }
    const std::string name = schema.at("name").get<std::string>();
    const std::size_t byte_width = schema.at("type").at("byteWidth").get<std::size_t>();
    const std::vector<std::vector<uint8_t>> data = array.at(DATA).get<std::vector<std::vector<uint8_t>>>();
    return sparrow::array{sparrow::fixed_width_binary_array{data, name}};
}

sparrow::array variable_size_binary_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    const std::string type = schema.at("type").at("name").get<std::string>();
    if (type != "utf8")
    {
        throw std::runtime_error("Invalid type");
    }
    const std::string name = schema.at("name").get<std::string>();
    const std::vector<std::string> data = array.at(DATA).get<std::vector<std::string>>();
    const std::vector<bool> validity = array.at(VALIDITY).get<std::vector<bool>>();
    return sparrow::array{sparrow::string_array{data, validity, name}};
}

sparrow::array timestamp_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    const std::string type = schema.at("type").at("name").get<std::string>();
    if (type != "timestamp")
    {
        throw std::runtime_error("Invalid type");
    }
    const std::string name = schema.at("name").get<std::string>();
    const std::string unit = schema.at("type").at("unit").get<std::string>();
    std::optional<std::string> timezone;
    if (schema.at("type").contains("timezone"))
    {
        timezone = schema.at("type").at("timezone").get<std::string>();
    }
    const date::time_zone* tz = timezone ? date::locate_zone(*timezone) : nullptr;
    const std::vector<int64_t> data = array.at(DATA).get<std::vector<int64_t>>();
    const std::vector<bool> validity = array.at(VALIDITY).get<std::vector<bool>>();
    if (unit == "SECOND")
    {
        return sparrow::array{sparrow::timestamp_seconds_array{tz, data, validity, name, timezone}};
    }
    else if (unit == "MILLISECOND")
    {
        return sparrow::array{sparrow::timestamp_milliseconds_array{tz, data, validity, name, timezone}};
    }
    else if (unit == "MICROSECOND")
    {
        return sparrow::array{sparrow::timestamp_microseconds_array{tz, data, validity, name, timezone}};
    }
    else if (unit == "NANOSECOND")
    {
        return sparrow::array{sparrow::timestamp_nanoseconds_array{tz, data, validity, name, timezone}};
    }
    else
    {
        throw std::runtime_error("Invalid unit");
    }
}

sparrow::array duration_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    const std::string type = schema.at("type").at("name").get<std::string>();
    if (type != "duration")
    {
        throw std::runtime_error("Invalid type");
    }
    const std::string name = schema.at("name").get<std::string>();
    const std::string unit = schema.at("type").at("unit").get<std::string>();
    const std::vector<int64_t> data = array.at(DATA).get<std::vector<int64_t>>();
    const std::vector<bool> validity = array.at(VALIDITY).get<std::vector<bool>>();
    if (unit == "SECOND")
    {
        return sparrow::array{sparrow::duration_seconds_array{data, validity, name}};
    }
    else if (unit == "MILLISECOND")
    {
        return sparrow::array{sparrow::duration_milliseconds_array{data, validity, name}};
    }
    else if (unit == "MICROSECOND")
    {
        return sparrow::array{sparrow::duration_microseconds_array{data, validity, name}};
    }
    else if (unit == "NANOSECOND")
    {
        return sparrow::array{sparrow::duration_nanoseconds_array{data, validity, name}};
    }
    else
    {
        throw std::runtime_error("Invalid unit");
    }
}

sparrow::array interval_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    const std::string type = schema.at("type").at("name").get<std::string>();
    if (type != "interval")
    {
        throw std::runtime_error("Invalid type");
    }
    const std::string name = schema.at("name").get<std::string>();
    const std::string unit = schema.at("type").at("unit").get<std::string>();
    const std::vector<int64_t> data = array.at(DATA).get<std::vector<int64_t>>();
    const std::vector<bool> validity = array.at(VALIDITY).get<std::vector<bool>>();
    if (unit == "YEAR_MONTH")
    {
        return sparrow::array{sparrow::months_interval_array{data, validity, name}};
    }
    else if (unit == "DAY_TIME")
    {
        return sparrow::array{sparrow::days_time_interval_array{data, validity, name}};
    }
    else
    {
        throw std::runtime_error("Invalid unit");
    }
}

sparrow::array union_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    const std::string type = schema.at("type").at("name").get<std::string>();
    if (type != "union")
    {
        throw std::runtime_error("Invalid type");
    }

    const std::string mode = schema.at("type").at("mode").get<std::string>();
}

sparrow::array sparse_union_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    const std::string type = schema.at("type").at("name").get<std::string>();
    if (type != "union")
    {
        throw std::runtime_error("Invalid type");
    }
    const std::string mode = schema.at("type").at("mode").get<std::string>();
    if (mode != "sparse")
    {
        throw std::runtime_error("Invalid mode");
    }
    const std::string name = schema.at("name").get<std::string>();
    const std::vector<uint8_t> type_ids_values = schema.at("type").at("typeIds").get<std::vector<uint8_t>>();
    const sparrow::sparse_union_array::type_id_buffer_type type_ids{
        type_ids_values.begin(),
        type_ids_values.end()
    };

    std::vector<bool> validity = array.at(VALIDITY).get < std::vector<bool>();
    const std::vector<nlohmann::json> children = schema.at("children").get<std::vector<nlohmann::json>>();
    std::vector<sparrow::array> arrays;
    arrays.reserve(children.size());
    for (const auto& child : children)
    {
        arrays.emplace_back(build_array_from_json(array, child));
    }
    return sparrow::array{sparrow::union_array{type_ids, offsets, arrays, validity, name}};
}

// sparrow::array fixedsizelist_from_json(const nlohmann::json& array, const nlohmann::json& schema)
// {
//     const std::string name = schema.at("name").get<std::string>();
//     const std::size_t list_size = schema.at("type").at("listSize").get<std::size_t>();

//     return sparrow::array{
//         sparrow::fixed_sized_list_array{array.at(DATA).get<std::vector<sparrow::fixed_size_list>>(), name}
//     };
// }

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

sparrow::array build_array_from_json(const nlohmann::json& array, const nlohmann::json& schema)
{
    const std::string type = schema.at("type").get<std::string>();
    return sparrow::array{};
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
