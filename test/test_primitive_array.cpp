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

#include <concepts>
#include <cstdint>

#include "sparrow/arrow_array_schema_proxy_factory.hpp"
#include "sparrow/layout/primitive_array.hpp"

#include "doctest/doctest.h"

namespace sparrow
{

    using testing_types = std::tuple<
        std::int8_t,
        std::uint8_t,
        std::int16_t,
        std::uint16_t,
        std::int32_t,
        std::uint32_t,
        std::int64_t,
        std::uint64_t,
        float16_t,
        float32_t,
        float64_t>;

    TEST_SUITE("primitive_array")
    {
        TEST_CASE_TEMPLATE_DEFINE("", T, primitive_array_id)
        {
            const std::array<T, 5> values{1, 2, 3, 4, 5};
            constexpr std::array<uint8_t, 1> nulls{2};
            constexpr int64_t offset = 1;

            auto make_array = [&values, &nulls]()
            {
                return make_primitive_arrow_proxy(values, nulls, offset, "test", std::nullopt);
            };

            // Elements: 2, null, 4, 5

            using array_test_type = primitive_array<T>;
            std::vector<T> values_z;

            SUBCASE("constructor")
            {
                const array_test_type ar(make_array());
                CHECK_EQ(ar.size(), 4);
            }

            SUBCASE("const operator[]")
            {
                const array_test_type ar(make_array());
                REQUIRE_EQ(ar.size(), 4);
                REQUIRE(ar[0].has_value());
                CHECK_EQ(ar[0].value(), values[1]);
                CHECK_FALSE(ar[1].has_value());
                REQUIRE(ar[2].has_value());
                CHECK_EQ(ar[2].value(), values[3]);
                REQUIRE(ar[3].has_value());
                CHECK_EQ(ar[3].value(), values[4]);
            }

            SUBCASE("value_iterator_ordering")
            {
                array_test_type ar(make_array());
                auto ar_values = ar.values();
                auto citer = ar_values.begin();
                CHECK(citer < ar_values.end());
            }

            SUBCASE("value_iterator_equality")
            {
                const array_test_type ar(make_array());
                const auto ar_values = ar.values();
                auto citer = ar_values.begin();
                CHECK_EQ(*citer, values[1]);
                ++citer;
                CHECK_EQ(*citer, values[2]);
                ++citer;
                CHECK_EQ(*citer, values[3]);
                ++citer;
                CHECK_EQ(*citer, values[4]);
                ++citer;
                CHECK_EQ(citer, ar_values.end());
            }

            SUBCASE("const_value_iterator_ordering")
            {
                array_test_type ar(make_array());
                auto ar_values = ar.values();
                auto citer = ar_values.begin();
                CHECK(citer < ar_values.end());
            }

            SUBCASE("const_value_iterator_equality")
            {
                array_test_type ar(make_array());
                auto ar_values = ar.values();
                for (std::size_t i = 0; i < ar.size(); ++i)
                {
                    if constexpr (std::same_as<T, float16_t>)
                    {
                        ar[i] = float16_t(static_cast<int32_t>(i));
                    }
                    else
                    {
                        ar[i] = static_cast<T>(i);
                    }
                }

                auto citer = ar_values.begin();
                for (std::size_t i = 0; i < ar.size(); ++i, ++citer)
                {
                    if constexpr (std::same_as<T, float16_t>)
                    {
                        CHECK_EQ(*citer, float16_t(static_cast<int32_t>(i)));
                    }
                    else
                    {
                        CHECK_EQ(*citer, i);
                    }
                }
            }

            SUBCASE("const_bitmap_iterator_ordering")
            {
                const array_test_type ar(make_array());
                const auto ar_bitmap = ar.bitmap();
                const auto citer = ar_bitmap.begin();
                CHECK(citer < ar_bitmap.end());
            }

            SUBCASE("const_bitmap_iterator_equality")
            {
                array_test_type ar(make_array());
                auto ar_bitmap = ar.bitmap();
                for (std::size_t i = 0; i < ar.size(); ++i)
                {
                    if (i % 2 != 0)
                    {
                        ar[i] = nullval;
                    }
                }

                auto citer = ar_bitmap.begin();
                for (std::size_t i = 0; i < ar.size(); ++i, ++citer)
                {
                    CHECK_EQ(*citer, i % 2 == 0);
                }
            }

            SUBCASE("iterator")
            {
                array_test_type ar(make_array());
                auto it = ar.begin();
                const auto end = ar.end();
                CHECK(it->has_value());
                CHECK_EQ(*it, values[1]);
                ++it;
                CHECK_FALSE(it->has_value());
                CHECK_EQ(*it, make_nullable(values[2], false));
                ++it;
                CHECK(it->has_value());
                CHECK_EQ(*it, make_nullable(values[3]));
                ++it;
                CHECK(it->has_value());
                CHECK_EQ(*it, make_nullable(values[4]));
                ++it;

                CHECK_EQ(it, end);

                const array_test_type ar_empty(
                    make_primitive_arrow_proxy(std::array<T, 0>{}, std::array<uint32_t, 0>{}, 0, "test", std::nullopt)
                );
                CHECK_EQ(ar_empty.begin(), ar_empty.end());
            }

            SUBCASE("resize")
            {
                array_test_type ar(make_array());
                const T new_value{99};
                ar.resize(7, 99);
                REQUIRE_EQ(ar.size(), 7);
                REQUIRE(ar[0].has_value());
                CHECK_EQ(ar[0].value(), values[1]);
                CHECK_FALSE(ar[1].has_value());
                REQUIRE(ar[2].has_value());
                CHECK_EQ(ar[2].value(), values[3]);
                REQUIRE(ar[3].has_value());
                CHECK_EQ(ar[3].value(), values[4]);
                REQUIRE(ar[4].has_value());
                CHECK_EQ(ar[4].value(), new_value);
                REQUIRE(ar[5].has_value());
                CHECK_EQ(ar[5].value(), new_value);
                REQUIRE(ar[6].has_value());

                const auto ar_bitmap = ar.bitmap();
                auto citer = ar_bitmap.begin();
                CHECK(*citer);
                ++citer;
                CHECK_FALSE(*citer);
                ++citer;
                CHECK(*citer);
                ++citer;
                CHECK(*citer);
                ++citer;
                CHECK(*citer);
                ++citer;
                CHECK(*citer);
                ++citer;
                CHECK(*citer);
                ++citer;
                CHECK_EQ(citer, ar_bitmap.end());
            }
        }
        TEST_CASE_TEMPLATE_APPLY(primitive_array_id, testing_types);
    }
}
