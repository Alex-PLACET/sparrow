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

#include <optional>

#include "sparrow/array.hpp"
#include "sparrow/layout/array_registry.hpp"
#include "sparrow/primitive_array.hpp"
#include "sparrow/union_array.hpp"
#include "sparrow/utils/nullable.hpp"

#include "../test/external_array_data_creation.hpp"
#include "doctest/doctest.h"
#include "test_utils.hpp"

namespace sparrow
{

    namespace test
    {
        template <class UnionArray>
        auto make_float32_typed_value(float32_t value, bool valid = true) -> typename UnionArray::typed_value
        {
            return
                typename UnionArray::typed_value(3, nullable<float32_t, bool>(value, static_cast<bool>(valid)));
        }

        template <class UnionArray>
        auto make_uint16_typed_value(std::uint16_t value, bool valid = true) -> typename UnionArray::typed_value
        {
            return typename UnionArray::typed_value(
                4,
                nullable<std::uint16_t, bool>(value, static_cast<bool>(valid))
            );
        }

        template <class UnionArray>
        auto get_float_child(const UnionArray& array) -> primitive_array<float32_t>
        {
            return primitive_array<float32_t>(
                ::sparrow::detail::array_access::get_arrow_proxy(array).children()[0].view()
            );
        }

        template <class UnionArray>
        auto get_uint16_child(const UnionArray& array) -> primitive_array<std::uint16_t>
        {
            return primitive_array<std::uint16_t>(
                ::sparrow::detail::array_access::get_arrow_proxy(array).children()[1].view()
            );
        }

        arrow_proxy
        make_sparse_union_proxy(const std::string& format_string, std::size_t n, bool altered = false)
        {
            std::vector<ArrowArray> children_arrays(2);
            std::vector<ArrowSchema> children_schemas(2);

            test::fill_schema_and_array<float32_t>(children_schemas[0], children_arrays[0], n, 0 /*offset*/, {});
            children_schemas[0].name = "item 0";

            test::fill_schema_and_array<std::uint16_t>(children_schemas[1], children_arrays[1], n, 0 /*offset*/, {});
            children_schemas[1].name = "item 1";

            ArrowArray arr{};
            ArrowSchema schema{};

            std::vector<std::uint8_t> type_ids =
                {std::uint8_t(3), std::uint8_t(4), std::uint8_t(3), std::uint8_t(4)};
            if (altered)
            {
                type_ids[0] = std::uint8_t(4);
            }

            test::fill_schema_and_array_for_sparse_union(
                schema,
                arr,
                std::move(children_schemas),
                std::move(children_arrays),
                type_ids,
                format_string
            );

            return arrow_proxy(std::move(arr), std::move(schema));
        }

        arrow_proxy
        make_dense_union_proxy(const std::string& format_string, std::size_t n_c, bool altered = false)
        {
            std::vector<ArrowArray> children_arrays(2);
            std::vector<ArrowSchema> children_schemas(2);

            test::fill_schema_and_array<float32_t>(children_schemas[0], children_arrays[0], n_c, 0 /*offset*/, {});
            children_schemas[0].name = "item 0";

            test::fill_schema_and_array<std::uint16_t>(
                children_schemas[1],
                children_arrays[1],
                n_c,
                0 /*offset*/,
                {}
            );
            children_schemas[1].name = "item 1";

            ArrowArray arr{};
            ArrowSchema schema{};

            std::vector<std::uint8_t> type_ids =
                {std::uint8_t(3), std::uint8_t(4), std::uint8_t(3), std::uint8_t(4)};
            if (altered)
            {
                type_ids[0] = std::uint8_t(4);
            }
            std::vector<std::int32_t> offsets = {0, 0, 1, 1};

            test::fill_schema_and_array_for_dense_union(
                schema,
                arr,
                std::move(children_schemas),
                std::move(children_arrays),
                type_ids,
                offsets,
                format_string
            );

            return arrow_proxy(std::move(arr), std::move(schema));
        }

        arrow_proxy make_noncanonical_dense_union_proxy()
        {
            std::vector<ArrowArray> children_arrays(2);
            std::vector<ArrowSchema> children_schemas(2);

            test::fill_schema_and_array<float32_t>(children_schemas[0], children_arrays[0], 2, 0 /*offset*/, {});
            children_schemas[0].name = "item 0";

            test::fill_schema_and_array<std::uint16_t>(children_schemas[1], children_arrays[1], 2, 0 /*offset*/, {});
            children_schemas[1].name = "item 1";

            ArrowArray arr{};
            ArrowSchema schema{};

            test::fill_schema_and_array_for_dense_union(
                schema,
                arr,
                std::move(children_schemas),
                std::move(children_arrays),
                std::vector<std::uint8_t>{std::uint8_t(3), std::uint8_t(4), std::uint8_t(3), std::uint8_t(4)},
                std::vector<std::int32_t>{1, 0, 0, 1},
                "+ud:3,4"
            );

            return arrow_proxy(std::move(arr), std::move(schema));
        }

        sparse_union_array make_non_nullable_sparse_union_array()
        {
            primitive_array<float32_t> arr1({float32_t(0.0f), float32_t(2.0f)}, false);
            primitive_array<std::uint16_t> arr2({std::uint16_t(1), std::uint16_t(3)}, false);

            std::vector<array> children = {array(std::move(arr1)), array(std::move(arr2))};
            sparse_union_array::type_id_buffer_type type_ids{{std::uint8_t(3), std::uint8_t(4)}};
            std::vector<std::size_t> type_mapping{3, 4};

            return sparse_union_array(
                std::move(children),
                std::move(type_ids),
                std::make_optional(std::move(type_mapping))
            );
        }

        sparse_union_array make_nullable_sparse_union_array()
        {
            primitive_array<float32_t> arr1({float32_t(0.0f), float32_t(2.0f)}, true);
            primitive_array<std::uint16_t> arr2({std::uint16_t(1), std::uint16_t(3)}, true);

            std::vector<array> children = {array(std::move(arr1)), array(std::move(arr2))};
            sparse_union_array::type_id_buffer_type type_ids{{std::uint8_t(3), std::uint8_t(4)}};
            std::vector<std::size_t> type_mapping{3, 4};

            return sparse_union_array(
                std::move(children),
                std::move(type_ids),
                std::make_optional(std::move(type_mapping))
            );
        }
    }

    TEST_SUITE("sparse_union")
    {
        static_assert(is_sparse_union_array_v<sparse_union_array>);
        static_assert(!is_dense_union_array_v<sparse_union_array>);

        TEST_CASE("constructor")
        {
            // the child arrays
            primitive_array<std::int16_t> arr1({{std::int16_t(2), std::int16_t(5), std::size_t(9)}});
            primitive_array<std::int32_t> arr2(
                std::vector<std::int32_t>{std::int32_t(3), std::int32_t(4), std::size_t(5)},
                std::vector<std::size_t>{1}  // INDEX 1 IS MISSING
            );

            // detyped arrays
            std::vector<array> children = {array(std::move(arr1)), array(std::move(arr2))};


            SUBCASE("with mapping")
            {
                // type ids
                sparse_union_array::type_id_buffer_type type_ids{
                    {std::uint8_t(2), std::uint8_t(3), std::uint8_t(3)}
                };

                // mapping
                std::vector<std::size_t> type_mapping{2, 3};

                // the array
                sparse_union_array arr(
                    std::move(children),
                    std::move(type_ids),
                    std::make_optional(std::move(type_mapping))
                );

                // check the size
                REQUIRE_EQ(arr.size(), 3);

                // check elements have values
                CHECK(arr[0].has_value());
                CHECK(!arr[1].has_value());
                CHECK(arr[2].has_value());

                CHECK_NULLABLE_VARIANT_EQ(arr[0], std::int16_t(2));
                CHECK_NULLABLE_VARIANT_EQ(arr[2], std::int32_t(5));
            }
            SUBCASE("without mapping")
            {
                // type ids
                sparse_union_array::type_id_buffer_type type_ids{
                    {std::uint8_t(0), std::uint8_t(1), std::uint8_t(1)}
                };

                // the array
                sparse_union_array arr(std::move(children), std::move(type_ids));

                // check the size
                REQUIRE_EQ(arr.size(), 3);

                // check elements have values
                CHECK(arr[0].has_value());
                CHECK(!arr[1].has_value());
                CHECK(arr[2].has_value());

                CHECK_NULLABLE_VARIANT_EQ(arr[0], std::int16_t(2));
                CHECK_NULLABLE_VARIANT_EQ(arr[2], std::int32_t(5));
            }
        }

        TEST_CASE("basics")
        {
            const std::string format_string = "+us:3,4";
            const std::size_t n = 4;


            auto proxy = test::make_sparse_union_proxy(format_string, n);
            sparse_union_array uarr(std::move(proxy));

            REQUIRE(uarr.size() == n);

            SUBCASE("copy")
            {
#ifdef SPARROW_TRACK_COPIES
                copy_tracker::reset(copy_tracker::key<sparse_union_array>());
#endif
                sparse_union_array uarr2(uarr);
                CHECK_EQ(uarr2, uarr);
#ifdef SPARROW_TRACK_COPIES
                CHECK_EQ(copy_tracker::count(copy_tracker::key<sparse_union_array>()), 1);
#endif

                sparse_union_array uarr3(test::make_sparse_union_proxy(format_string, n, true));
                CHECK_NE(uarr3, uarr);
                uarr3 = uarr;
                CHECK_EQ(uarr3, uarr);
            }

            SUBCASE("move")
            {
                sparse_union_array uarr2(uarr);
                sparse_union_array uarr3(std::move(uarr2));
                CHECK_EQ(uarr3, uarr);

                sparse_union_array uarr4(test::make_sparse_union_proxy(format_string, n, true));
                CHECK_NE(uarr4, uarr);
                uarr4 = std::move(uarr3);
                CHECK_EQ(uarr4, uarr);
            }

            SUBCASE("operator[]")
            {
                for (std::size_t i = 0; i < n; ++i)
                {
                    const auto& val = uarr[i];
                    REQUIRE(val.has_value());
                }

#if SPARROW_GCC_11_2_WORKAROUND
                using variant_type = std::decay_t<decltype(uarr[0])>;
                using base_type = typename variant_type::base_type;
#endif
                // 0
                std::visit(
                    [](auto&& arg)
                    {
                        using inner_type = std::decay_t<typename std::decay_t<decltype(arg)>::value_type>;
                        if constexpr (std::is_same_v<inner_type, float32_t>)
                        {
                            REQUIRE_EQ(0.0f, arg.value());
                        }
                        else
                        {
                            CHECK(false);
                        }
                    },

#if SPARROW_GCC_11_2_WORKAROUND
                    static_cast<const base_type&>(uarr[0])
#else
                    uarr[0]
#endif
                );

                // 1
                std::visit(
                    [](auto&& arg)
                    {
                        using inner_type = std::decay_t<typename std::decay_t<decltype(arg)>::value_type>;
                        if constexpr (std::is_same_v<inner_type, std::uint16_t>)
                        {
                            REQUIRE_EQ(1, arg.value());
                        }
                        else
                        {
                            CHECK(false);
                        }
                    },
#if SPARROW_GCC_11_2_WORKAROUND
                    static_cast<const base_type&>(uarr[1])
#else
                    uarr[1]
#endif
                );

                // 2
                std::visit(
                    [](auto&& arg)
                    {
                        using inner_type = std::decay_t<typename std::decay_t<decltype(arg)>::value_type>;
                        if constexpr (std::is_same_v<inner_type, float32_t>)
                        {
                            REQUIRE_EQ(2.0f, arg.value());
                        }
                        else
                        {
                            CHECK(false);
                        }
                    },
#if SPARROW_GCC_11_2_WORKAROUND
                    static_cast<const base_type&>(uarr[2])
#else
                    uarr[2]
#endif
                );

                // 3
                std::visit(
                    [](auto&& arg)
                    {
                        using inner_type = std::decay_t<typename std::decay_t<decltype(arg)>::value_type>;
                        if constexpr (std::is_same_v<inner_type, std::uint16_t>)
                        {
                            REQUIRE_EQ(3, arg.value());
                        }
                        else
                        {
                            CHECK(false);
                        }
                    },
#if SPARROW_GCC_11_2_WORKAROUND
                    static_cast<const base_type&>(uarr[3])
#else
                    uarr[3]
#endif
                );
            }
        }

#if defined(__cpp_lib_format)
        TEST_CASE("formatting")
        {
            const std::string format_string = "+us:3,4";
            const std::size_t n = 4;

            auto proxy = test::make_sparse_union_proxy(format_string, n);
            sparse_union_array uarr(std::move(proxy));

            const std::string formatted = std::format("{}", uarr);
            constexpr std::string_view expected = "SparseUnion [name=test | size=4] <0, 1, 2, 3>";
            CHECK_EQ(formatted, expected);
        }
#endif
    }

    TEST_SUITE("sparse_union_mutable")
    {
        TEST_CASE("insert fills inactive nullable children with null")
        {
            auto arr = test::make_nullable_sparse_union_array();

            static_cast<void>(arr.insert(
                sparrow::next(arr.cbegin(), 1),
                test::make_uint16_typed_value<sparse_union_array>(std::uint16_t(99))
            ));

            REQUIRE_EQ(arr.size(), 3u);
            CHECK_NULLABLE_VARIANT_EQ(arr[1], std::uint16_t(99));

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            REQUIRE_EQ(child0.size(), 3u);
            REQUIRE_EQ(child1.size(), 3u);
            CHECK_FALSE(child0[1].has_value());
            CHECK(child1[1].has_value());
            CHECK_EQ(child1[1].value(), std::uint16_t(99));
        }

        TEST_CASE("insert fills inactive non nullable children with defaults")
        {
            auto arr = test::make_non_nullable_sparse_union_array();

            static_cast<void>(arr.insert(
                sparrow::next(arr.cbegin(), 1),
                test::make_uint16_typed_value<sparse_union_array>(std::uint16_t(99))
            ));

            REQUIRE_EQ(arr.size(), 3u);
            CHECK_NULLABLE_VARIANT_EQ(arr[1], std::uint16_t(99));

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            REQUIRE_EQ(child0.size(), 3u);
            REQUIRE_EQ(child1.size(), 3u);
            CHECK(child0[1].has_value());
            CHECK_EQ(child0[1].value(), 0.0f);
            CHECK(child1[1].has_value());
            CHECK_EQ(child1[1].value(), std::uint16_t(99));
        }

        TEST_CASE("push_back appends value")
        {
            auto arr = test::make_nullable_sparse_union_array();

            arr.push_back(test::make_float32_typed_value<sparse_union_array>(42.0f));

            REQUIRE_EQ(arr.size(), 3u);
            CHECK_NULLABLE_VARIANT_EQ(arr[2], float32_t(42.0f));

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            REQUIRE_EQ(child0.size(), 3u);
            REQUIRE_EQ(child1.size(), 3u);
            CHECK(child0[2].has_value());
            CHECK_EQ(child0[2].value(), 42.0f);
            CHECK_FALSE(child1[2].has_value());
        }

        TEST_CASE("pop_back removes last value")
        {
            auto proxy = test::make_sparse_union_proxy("+us:3,4", 4);
            sparse_union_array arr(std::move(proxy));

            arr.pop_back();

            REQUIRE_EQ(arr.size(), 3u);
            CHECK_NULLABLE_VARIANT_EQ(arr[0], float32_t(0.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[1], std::uint16_t(1));
            CHECK_NULLABLE_VARIANT_EQ(arr[2], float32_t(2.0f));

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            REQUIRE_EQ(child0.size(), 3u);
            REQUIRE_EQ(child1.size(), 3u);
            CHECK_EQ(child0[2].value(), 2.0f);
            CHECK_EQ(child1[2].value(), std::uint16_t(2));
        }

        TEST_CASE("erase removes value and compacts children")
        {
            auto proxy = test::make_sparse_union_proxy("+us:3,4", 4);
            sparse_union_array arr(std::move(proxy));

            static_cast<void>(arr.erase(sparrow::next(arr.cbegin(), 1)));
            REQUIRE_EQ(arr.size(), 3u);
            CHECK_NULLABLE_VARIANT_EQ(arr[0], float32_t(0.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[1], float32_t(2.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[2], std::uint16_t(3));

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            REQUIRE_EQ(child0.size(), 3u);
            REQUIRE_EQ(child1.size(), 3u);
            CHECK_EQ(child0[0].value(), 0.0f);
            CHECK_EQ(child0[1].value(), 2.0f);
            CHECK_EQ(child0[2].value(), 3.0f);
            CHECK_EQ(child1[0].value(), std::uint16_t(0));
            CHECK_EQ(child1[1].value(), std::uint16_t(2));
            CHECK_EQ(child1[2].value(), std::uint16_t(3));
        }

        TEST_CASE("resize grows with fill value")
        {
            auto proxy = test::make_sparse_union_proxy("+us:3,4", 4);
            sparse_union_array arr(std::move(proxy));

            arr.resize(6, test::make_uint16_typed_value<sparse_union_array>(std::uint16_t(11)));

            REQUIRE_EQ(arr.size(), 6u);
            CHECK_NULLABLE_VARIANT_EQ(arr[4], std::uint16_t(11));
            CHECK_NULLABLE_VARIANT_EQ(arr[5], std::uint16_t(11));

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            REQUIRE_EQ(child0.size(), 6u);
            REQUIRE_EQ(child1.size(), 6u);
        }

        TEST_CASE("resize shrinks array")
        {
            auto proxy = test::make_sparse_union_proxy("+us:3,4", 4);
            sparse_union_array arr(std::move(proxy));

            arr.resize(2, test::make_uint16_typed_value<sparse_union_array>(std::uint16_t(11)));

            REQUIRE_EQ(arr.size(), 2u);
            CHECK_NULLABLE_VARIANT_EQ(arr[0], float32_t(0.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[1], std::uint16_t(1));

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            REQUIRE_EQ(child0.size(), 2u);
            REQUIRE_EQ(child1.size(), 2u);
        }

        TEST_CASE("clear removes all values")
        {
            auto proxy = test::make_sparse_union_proxy("+us:3,4", 4);
            sparse_union_array arr(std::move(proxy));

            arr.clear();
            CHECK(arr.empty());

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            CHECK(child0.empty());
            CHECK(child1.empty());
        }

        TEST_CASE("insert on slice is unsupported")
        {
            sparse_union_array original(test::make_sparse_union_proxy("+us:3,4", 4));
            sparse_union_array sliced(detail::array_access::get_arrow_proxy(original).slice(1, 2));

            CHECK_THROWS_AS(
                sliced.insert(sliced.cbegin(), test::make_float32_typed_value<sparse_union_array>(7.0f)),
                std::logic_error
            );
        }

        TEST_CASE("erase on slice is unsupported")
        {
            sparse_union_array original(test::make_sparse_union_proxy("+us:3,4", 4));
            sparse_union_array sliced(detail::array_access::get_arrow_proxy(original).slice(1, 2));

            CHECK_THROWS_AS(sliced.erase(sliced.cbegin()), std::logic_error);
        }
    }

    TEST_SUITE("dense_union")
    {
        static_assert(is_dense_union_array_v<dense_union_array>);
        static_assert(!is_sparse_union_array_v<dense_union_array>);
        TEST_CASE("constructor")
        {
            // the child arrays
            primitive_array<std::int16_t> arr1({{std::int16_t(0), std::int16_t(1)}});
            primitive_array<std::int32_t> arr2(
                std::vector<std::int32_t>{std::int32_t(2), std::int32_t(3)},
                std::vector<std::size_t>{1}  // INDEX 1 IS MISSING
            );

            // detyped arrays
            std::vector<array> children = {array(std::move(arr1)), array(std::move(arr2))};

            // offsets
            dense_union_array::offset_buffer_type offsets{
                {std::size_t(1), std::size_t(1), std::size_t(0), std::size_t(0)}
            };

            SUBCASE("without mapping")
            {
                // type ids
                dense_union_array::type_id_buffer_type type_ids{
                    {std::uint8_t(0), std::uint8_t(1), std::uint8_t(0), std::uint8_t(1)}
                };

                // the array
                dense_union_array arr(std::move(children), std::move(type_ids), std::move(offsets));

                // check the size
                REQUIRE_EQ(arr.size(), 4);

                // check elements have values
                CHECK(arr[0].has_value());
                CHECK(!arr[1].has_value());
                CHECK(arr[2].has_value());
                CHECK(arr[3].has_value());

                CHECK_NULLABLE_VARIANT_EQ(arr[0], std::int16_t(1));
                CHECK_NULLABLE_VARIANT_EQ(arr[2], std::int16_t(0));
                CHECK_NULLABLE_VARIANT_EQ(arr[3], std::int32_t(2));
            }
            SUBCASE("with mapping")
            {
                std::vector<std::size_t> child_index_to_type_id{1, 0};
                // type ids
                dense_union_array::type_id_buffer_type type_ids{
                    {std::uint8_t(1), std::uint8_t(0), std::uint8_t(1), std::uint8_t(0)}
                };

                // the array
                dense_union_array arr(
                    std::move(children),
                    std::move(type_ids),
                    std::move(offsets),
                    std::make_optional(std::move(child_index_to_type_id))
                );

                // check the size
                REQUIRE_EQ(arr.size(), 4);

                // check elements have values
                CHECK(arr[0].has_value());
                CHECK(!arr[1].has_value());
                CHECK(arr[2].has_value());
                CHECK(arr[3].has_value());

                CHECK_NULLABLE_VARIANT_EQ(arr[0], std::int16_t(1));
                CHECK_NULLABLE_VARIANT_EQ(arr[2], std::int16_t(0));
                CHECK_NULLABLE_VARIANT_EQ(arr[3], std::int32_t(2));
            }
        }
        TEST_CASE("basics")
        {
            const std::string format_string = "+ud:3,4";
            const std::size_t n_c = 2;
            const std::size_t n = 4;

            auto proxy = test::make_dense_union_proxy(format_string, n_c);
            dense_union_array uarr(std::move(proxy));

            REQUIRE(uarr.size() == n);

            SUBCASE("copy")
            {
#ifdef SPARROW_TRACK_COPIES
                copy_tracker::reset(copy_tracker::key<dense_union_array>());
#endif
                dense_union_array uarr2(uarr);
                CHECK_EQ(uarr2, uarr);
#ifdef SPARROW_TRACK_COPIES
                CHECK_EQ(copy_tracker::count(copy_tracker::key<dense_union_array>()), 1);
#endif

                dense_union_array uarr3(test::make_dense_union_proxy(format_string, n_c, true));
                CHECK_NE(uarr3, uarr);
                uarr3 = uarr;
                CHECK_EQ(uarr3, uarr);
            }

            SUBCASE("move")
            {
                dense_union_array uarr2(uarr);
                dense_union_array uarr3(std::move(uarr2));
                CHECK_EQ(uarr3, uarr);

                dense_union_array uarr4(test::make_dense_union_proxy(format_string, n_c, true));
                CHECK_NE(uarr4, uarr);
                uarr4 = std::move(uarr3);
                CHECK_EQ(uarr4, uarr);
            }

            SUBCASE("operator[]")
            {
                for (std::size_t i = 0; i < n; ++i)
                {
                    const auto& val = uarr[i];
                    REQUIRE(val.has_value());
                }
            }

#if SPARROW_GCC_11_2_WORKAROUND
            using variant_type = std::decay_t<decltype(uarr[0])>;
            using base_type = typename variant_type::base_type;
#endif

            // 0
            std::visit(
                [](auto&& arg)
                {
                    using inner_type = std::decay_t<typename std::decay_t<decltype(arg)>::value_type>;
                    if constexpr (std::is_same_v<inner_type, float32_t>)
                    {
                        REQUIRE_EQ(0.0f, arg.value());
                    }
                    else
                    {
                        CHECK(false);
                    }
                },
#if SPARROW_GCC_11_2_WORKAROUND
                static_cast<const base_type&>(uarr[0])
#else
                uarr[0]
#endif
            );

            // 1
            std::visit(
                [](auto&& arg)
                {
                    using inner_type = std::decay_t<typename std::decay_t<decltype(arg)>::value_type>;
                    if constexpr (std::is_same_v<inner_type, std::uint16_t>)
                    {
                        REQUIRE_EQ(0, arg.value());
                    }
                    else
                    {
                        CHECK(false);
                    }
                },
#if SPARROW_GCC_11_2_WORKAROUND
                static_cast<const base_type&>(uarr[1])
#else
                uarr[1]
#endif
            );

            // 2
            std::visit(
                [](auto&& arg)
                {
                    using inner_type = std::decay_t<typename std::decay_t<decltype(arg)>::value_type>;
                    if constexpr (std::is_same_v<inner_type, float32_t>)
                    {
                        REQUIRE_EQ(1.0f, arg.value());
                    }
                    else
                    {
                        CHECK(false);
                    }
                },
#if SPARROW_GCC_11_2_WORKAROUND
                static_cast<const base_type&>(uarr[2])
#else
                uarr[2]
#endif
            );

            // 3
            std::visit(
                [](auto&& arg)
                {
                    using inner_type = std::decay_t<typename std::decay_t<decltype(arg)>::value_type>;
                    if constexpr (std::is_same_v<inner_type, std::uint16_t>)
                    {
                        REQUIRE_EQ(1, arg.value());
                    }
                    else
                    {
                        CHECK(false);
                    }
                },
#if SPARROW_GCC_11_2_WORKAROUND
                static_cast<const base_type&>(uarr[3])
#else
                uarr[3]
#endif
            );
        }

#if defined(__cpp_lib_format)
        TEST_CASE("formatting")
        {
            const std::string format_string = "+ud:3,4";
            const std::size_t n_c = 2;

            auto proxy = test::make_dense_union_proxy(format_string, n_c);
            dense_union_array uarr(std::move(proxy));

            const std::string formatted = std::format("{}", uarr);
            constexpr std::string_view expected = "DenseUnion [name=test | size=4] <0, 0, 1, 1>";
            CHECK_EQ(formatted, expected);
        }
#endif
    }

    TEST_SUITE("dense_union_mutable")
    {
        TEST_CASE("insert recomputes dense offsets")
        {
            auto proxy = test::make_dense_union_proxy("+ud:3,4", 2);
            dense_union_array arr(std::move(proxy));

            static_cast<void>(arr.insert(
                sparrow::next(arr.cbegin(), 1),
                test::make_float32_typed_value<dense_union_array>(9.5f)
            ));

            REQUIRE_EQ(arr.size(), 5u);
            CHECK_NULLABLE_VARIANT_EQ(arr[0], float32_t(0.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[1], float32_t(9.5f));
            CHECK_NULLABLE_VARIANT_EQ(arr[2], std::uint16_t(0));
            CHECK_NULLABLE_VARIANT_EQ(arr[3], float32_t(1.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[4], std::uint16_t(1));

            {
                const auto child0 = test::get_float_child(arr);
                const auto child1 = test::get_uint16_child(arr);
                REQUIRE_EQ(child0.size(), 3u);
                REQUIRE_EQ(child1.size(), 2u);
                CHECK_EQ(child0[0].value(), 0.0f);
                CHECK_EQ(child0[1].value(), 9.5f);
                CHECK_EQ(child0[2].value(), 1.0f);
                CHECK_EQ(child1[0].value(), std::uint16_t(0));
                CHECK_EQ(child1[1].value(), std::uint16_t(1));
            }
        }

        TEST_CASE("push_back appends value and recomputes dense offsets")
        {
            auto proxy = test::make_dense_union_proxy("+ud:3,4", 2);
            dense_union_array arr(std::move(proxy));

            arr.push_back(test::make_uint16_typed_value<dense_union_array>(std::uint16_t(77)));

            REQUIRE_EQ(arr.size(), 5u);
            CHECK_NULLABLE_VARIANT_EQ(arr[0], float32_t(0.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[1], std::uint16_t(0));
            CHECK_NULLABLE_VARIANT_EQ(arr[2], float32_t(1.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[3], std::uint16_t(1));
            CHECK_NULLABLE_VARIANT_EQ(arr[4], std::uint16_t(77));

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            REQUIRE_EQ(child0.size(), 2u);
            REQUIRE_EQ(child1.size(), 3u);
            CHECK_EQ(child1[0].value(), std::uint16_t(0));
            CHECK_EQ(child1[1].value(), std::uint16_t(1));
            CHECK_EQ(child1[2].value(), std::uint16_t(77));
        }

        TEST_CASE("pop_back removes last value and recomputes dense offsets")
        {
            auto proxy = test::make_dense_union_proxy("+ud:3,4", 2);
            dense_union_array arr(std::move(proxy));

            arr.pop_back();

            REQUIRE_EQ(arr.size(), 3u);
            CHECK_NULLABLE_VARIANT_EQ(arr[0], float32_t(0.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[1], std::uint16_t(0));
            CHECK_NULLABLE_VARIANT_EQ(arr[2], float32_t(1.0f));

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            REQUIRE_EQ(child0.size(), 2u);
            REQUIRE_EQ(child1.size(), 1u);
            CHECK_EQ(child1[0].value(), std::uint16_t(0));
        }

        TEST_CASE("erase recomputes dense offsets")
        {
            auto proxy = test::make_dense_union_proxy("+ud:3,4", 2);
            dense_union_array arr(std::move(proxy));

            static_cast<void>(arr.erase(sparrow::next(arr.cbegin(), 2)));

            REQUIRE_EQ(arr.size(), 3u);
            CHECK_NULLABLE_VARIANT_EQ(arr[0], float32_t(0.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[1], std::uint16_t(0));
            CHECK_NULLABLE_VARIANT_EQ(arr[2], std::uint16_t(1));

            {
                const auto child0 = test::get_float_child(arr);
                const auto child1 = test::get_uint16_child(arr);
                REQUIRE_EQ(child0.size(), 1u);
                REQUIRE_EQ(child1.size(), 2u);
                CHECK_EQ(child0[0].value(), 0.0f);
                CHECK_EQ(child1[0].value(), std::uint16_t(0));
                CHECK_EQ(child1[1].value(), std::uint16_t(1));
            }
        }

        TEST_CASE("resize shrinks dense union")
        {
            auto proxy = test::make_dense_union_proxy("+ud:3,4", 2);
            dense_union_array arr(std::move(proxy));

            arr.resize(3, test::make_uint16_typed_value<dense_union_array>(std::uint16_t(11)));

            REQUIRE_EQ(arr.size(), 3u);
            CHECK_NULLABLE_VARIANT_EQ(arr[0], float32_t(0.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[1], std::uint16_t(0));
            CHECK_NULLABLE_VARIANT_EQ(arr[2], float32_t(1.0f));

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            REQUIRE_EQ(child0.size(), 2u);
            REQUIRE_EQ(child1.size(), 1u);
        }

        TEST_CASE("resize grows dense union with fill value")
        {
            auto proxy = test::make_dense_union_proxy("+ud:3,4", 2);
            dense_union_array arr(std::move(proxy));

            arr.resize(6, test::make_uint16_typed_value<dense_union_array>(std::uint16_t(11)));

            REQUIRE_EQ(arr.size(), 6u);
            CHECK_NULLABLE_VARIANT_EQ(arr[0], float32_t(0.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[1], std::uint16_t(0));
            CHECK_NULLABLE_VARIANT_EQ(arr[2], float32_t(1.0f));
            CHECK_NULLABLE_VARIANT_EQ(arr[3], std::uint16_t(1));
            CHECK_NULLABLE_VARIANT_EQ(arr[4], std::uint16_t(11));
            CHECK_NULLABLE_VARIANT_EQ(arr[5], std::uint16_t(11));

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            REQUIRE_EQ(child0.size(), 2u);
            REQUIRE_EQ(child1.size(), 4u);
            CHECK_EQ(child1[2].value(), std::uint16_t(11));
            CHECK_EQ(child1[3].value(), std::uint16_t(11));
        }

        TEST_CASE("clear removes all dense union values")
        {
            auto proxy = test::make_dense_union_proxy("+ud:3,4", 2);
            dense_union_array arr(std::move(proxy));

            arr.clear();
            CHECK(arr.empty());

            const auto child0 = test::get_float_child(arr);
            const auto child1 = test::get_uint16_child(arr);
            CHECK(child0.empty());
            CHECK(child1.empty());
        }

        TEST_CASE("insert on non canonical offsets is unsupported")
        {
            dense_union_array arr(test::make_noncanonical_dense_union_proxy());

            CHECK_THROWS_AS(
                arr.insert(arr.cbegin(), test::make_uint16_typed_value<dense_union_array>(std::uint16_t(9))),
                std::logic_error
            );
        }

        TEST_CASE("erase on non canonical offsets is unsupported")
        {
            dense_union_array arr(test::make_noncanonical_dense_union_proxy());

            CHECK_THROWS_AS(arr.erase(arr.cbegin()), std::logic_error);
        }

        TEST_CASE("insert on slice is unsupported")
        {
            dense_union_array original(test::make_dense_union_proxy("+ud:3,4", 2));
            dense_union_array sliced(detail::array_access::get_arrow_proxy(original).slice(1, 2));

            CHECK_THROWS_AS(
                sliced.insert(sliced.cbegin(), test::make_float32_typed_value<dense_union_array>(7.0f)),
                std::logic_error
            );
        }

        TEST_CASE("erase on slice is unsupported")
        {
            dense_union_array original(test::make_dense_union_proxy("+ud:3,4", 2));
            dense_union_array sliced(detail::array_access::get_arrow_proxy(original).slice(1, 2));

            CHECK_THROWS_AS(sliced.erase(sliced.cbegin()), std::logic_error);
        }
    }
}
