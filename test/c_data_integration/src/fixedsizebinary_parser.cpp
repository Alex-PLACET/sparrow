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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "sparrow/c_data_integration/fixedsizebinary_parser.hpp"

#include "sparrow/c_data_integration/constant.hpp"
#include "sparrow/c_data_integration/utils.hpp"

namespace sparrow::c_data_integration
{
    sparrow::array
    fixedsizebinary_from_json(const nlohmann::json& array, const nlohmann::json& schema, const nlohmann::json&)
    {
        utils::check_type(schema, "fixedsizebinary");
        const std::string name = schema.at("name").get<std::string>();
        const std::size_t byte_width = schema.at("type").at("byteWidth").get<std::size_t>();
        auto data_str = array.at(DATA).get<std::vector<std::string>>();
        auto data = utils::hexStringsToBytes(data_str);
        auto metadata = utils::get_metadata(schema);
        if (data.empty())
        {
            sparrow::u8_buffer<std::byte> data_buffer(0);
            sparrow::fixed_width_binary_array
                ar{std::move(data_buffer), byte_width, std::array<bool, 0>{}, name, std::move(metadata)};
            return sparrow::array{std::move(ar)};
        }
        else
        {
            auto validity = utils::get_validity(array);
            sparrow::fixed_width_binary_array ar{std::move(data), std::move(validity), name, std::move(metadata)};
            return sparrow::array{std::move(ar)};
        }
    }
}
