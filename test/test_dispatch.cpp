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

#include "sparrow/array.hpp"
#include "sparrow/layout/array_helper.hpp"
#include "sparrow/primitive_array.hpp"
#include "sparrow/variable_size_binary_array.hpp"

#include "../test/external_array_data_creation.hpp"
#include "doctest/doctest.h"

namespace sparrow
{
    using test::make_arrow_proxy;
    using testing_types = std::tuple<
        null_array,
        primitive_array<std::int8_t>,
        primitive_array<std::uint8_t>,
        primitive_array<std::int16_t>,
        primitive_array<std::uint16_t>,
        primitive_array<std::int32_t>,
        primitive_array<std::uint32_t>,
        primitive_array<std::int64_t>,
        primitive_array<std::uint64_t>,
        primitive_array<float16_t>,
        primitive_array<float32_t>,
        primitive_array<float64_t>>;

    TEST_SUITE("dispatch")
    {
        TEST_CASE_TEMPLATE_DEFINE("array_size", AR, array_size_id)
        {
            using array_type = AR;
            using wrapper_type = array_wrapper_impl<AR>;
            array_type ar(make_arrow_proxy<typename AR::inner_value_type>());
            wrapper_type w(&ar);
            auto size = array_size(w);
            CHECK_EQ(size, ar.size());
        }

        TEST_CASE_TEMPLATE_APPLY(array_size_id, testing_types);

        TEST_CASE_TEMPLATE_DEFINE("array_element", AR, array_element_id)
        {
            using array_type = AR;
            using wrapper_type = array_wrapper_impl<AR>;
            array_type ar(make_arrow_proxy<typename AR::inner_value_type>());
            wrapper_type w(&ar);

            for (std::size_t i = 0; i < ar.size(); ++i)
            {
                auto elem = array_element(w, i);
                CHECK_EQ(elem.has_value(), ar[i].has_value());
                if (elem.has_value())
                {
                    CHECK_EQ(std::get<typename AR::const_reference>(elem).value(), ar[i].value());
                }
            }
        }

        TEST_CASE_TEMPLATE_APPLY(array_element_id, testing_types);

        TEST_CASE("array wrapper helpers")
        {
            primitive_array<std::int32_t> typed_array(
                std::vector<nullable<std::int32_t>>{make_nullable(42), make_nullable(std::int32_t{}, false)}
            );
            array_wrapper_impl<primitive_array<std::int32_t>> wrapper(&typed_array);

            CHECK(array_has_value(wrapper, 0));
            CHECK_FALSE(array_has_value(wrapper, 1));
            CHECK_EQ(std::get<std::int32_t>(array_default_element_value(wrapper)), 0);
            const auto default_value = array_default_value(wrapper);
            CHECK(std::get<nullable<std::int32_t>>(default_value).has_value());
            CHECK_EQ(std::get<nullable<std::int32_t>>(default_value).value(), 0);

            const auto view = make_array_view(wrapper);
            CHECK_EQ(view.size(), typed_array.size());
            CHECK_EQ(std::get<primitive_array<std::int32_t>::const_reference>(view[0]).value(), 42);
        }

        TEST_CASE("array materialization and rebuilding helpers")
        {
            array source(string_array(std::vector<nullable<std::string>>{make_nullable(std::string("one"))}));
            const auto materialized = array_materialize_element(source[0]);
            const auto& text = std::get<nullable<std::string>>(materialized).value();
            CHECK_EQ(text, "one");

            const auto snapshot = snapshot_array(source);
            CHECK_EQ(snapshot.size(), 1);
            CHECK_EQ(std::get<nullable<std::string>>(snapshot[0]).value(), "one");

            array rebuilt = array_empty_like(source);
            CHECK(rebuilt.empty());
            append_values(rebuilt, snapshot);
            CHECK_EQ(rebuilt.size(), 1);
            CHECK_EQ(std::get<string_array::const_reference>(rebuilt[0]).value(), "one");

            const auto single_element = array_make_from_element(materialized);
            CHECK_EQ(single_element.size(), 1);
            CHECK_EQ(std::get<string_array::const_reference>(single_element[0]).value(), "one");
        }
    }
}
