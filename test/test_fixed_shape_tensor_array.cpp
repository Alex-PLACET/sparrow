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

#include "sparrow/fixed_shape_tensor_array.hpp"
#include "sparrow/primitive_array.hpp"

#include "doctest/doctest.h"
#include "test_utils.hpp"

namespace sparrow
{
    TEST_SUITE("fixed_shape_tensor_array")
    {
        TEST_CASE("type_traits")
        {
            static_assert(is_fixed_shape_tensor_array_v<fixed_shape_tensor_array>);
        }

        TEST_CASE("constructor_from_shape_and_flat_array")
        {
            SUBCASE("2D tensors")
            {
                // Create 3 tensors of shape [2, 3] (6 elements each)
                std::vector<int64_t> shape = {2, 3};
                std::vector<double> flat_data = {
                    // Tensor 0
                    1.0, 2.0, 3.0,
                    4.0, 5.0, 6.0,
                    // Tensor 1
                    7.0, 8.0, 9.0,
                    10.0, 11.0, 12.0,
                    // Tensor 2
                    13.0, 14.0, 15.0,
                    16.0, 17.0, 18.0
                };

                primitive_array<double> flat_arr(flat_data);
                fixed_shape_tensor_array tensor_arr(
                    shape,
                    array(std::move(flat_arr)),
                    false  // not nullable
                );

                CHECK_EQ(tensor_arr.size(), 3);
                CHECK_EQ(tensor_arr.shape().size(), 2);
                CHECK_EQ(tensor_arr.shape()[0], 2);
                CHECK_EQ(tensor_arr.shape()[1], 3);
                CHECK_FALSE(tensor_arr.is_column_major());
                CHECK_FALSE(tensor_arr.dim_names().has_value());

                // Check first tensor
                auto tensor0 = tensor_arr[0].value();
                CHECK_EQ(tensor0.size(), 6);
                for (size_t i = 0; i < 6; ++i)
                {
                    CHECK_NULLABLE_VARIANT_EQ(tensor0[i], flat_data[i]);
                }

                // Check second tensor
                auto tensor1 = tensor_arr[1].value();
                CHECK_EQ(tensor1.size(), 6);
                for (size_t i = 0; i < 6; ++i)
                {
                    CHECK_NULLABLE_VARIANT_EQ(tensor1[i], flat_data[6 + i]);
                }
            }

            SUBCASE("3D tensors")
            {
                // Create 2 tensors of shape [2, 2, 2] (8 elements each)
                std::vector<int64_t> shape = {2, 2, 2};
                std::vector<float> flat_data(16);
                std::iota(flat_data.begin(), flat_data.end(), 1.0f);

                primitive_array<float> flat_arr(flat_data);
                fixed_shape_tensor_array tensor_arr(
                    shape,
                    array(std::move(flat_arr)),
                    true  // nullable
                );

                CHECK_EQ(tensor_arr.size(), 2);
                CHECK_EQ(tensor_arr.shape().size(), 3);
                CHECK_EQ(tensor_arr.shape()[0], 2);
                CHECK_EQ(tensor_arr.shape()[1], 2);
                CHECK_EQ(tensor_arr.shape()[2], 2);
            }

            SUBCASE("1D tensors (vectors)")
            {
                // Create 5 tensors of shape [4] (4 elements each)
                std::vector<int64_t> shape = {4};
                std::vector<int32_t> flat_data(20);
                std::iota(flat_data.begin(), flat_data.end(), 0);

                primitive_array<int32_t> flat_arr(flat_data);
                fixed_shape_tensor_array tensor_arr(
                    shape,
                    array(std::move(flat_arr)),
                    false
                );

                CHECK_EQ(tensor_arr.size(), 5);
                CHECK_EQ(tensor_arr.shape().size(), 1);
                CHECK_EQ(tensor_arr.shape()[0], 4);

                // Check each tensor
                for (size_t i = 0; i < 5; ++i)
                {
                    auto tensor = tensor_arr[i].value();
                    CHECK_EQ(tensor.size(), 4);
                    for (size_t j = 0; j < 4; ++j)
                    {
                        CHECK_NULLABLE_VARIANT_EQ(tensor[j], static_cast<int32_t>(i * 4 + j));
                    }
                }
            }
        }

        TEST_CASE("constructor_with_dimension_names")
        {
            std::vector<int64_t> shape = {3, 4};
            std::vector<std::string> dim_names = {"rows", "cols"};
            std::vector<double> flat_data(24);  // 2 tensors of 3x4
            std::iota(flat_data.begin(), flat_data.end(), 1.0);

            primitive_array<double> flat_arr(flat_data);
            fixed_shape_tensor_array tensor_arr(
                shape,
                array(std::move(flat_arr)),
                false,
                dim_names
            );

            CHECK_EQ(tensor_arr.size(), 2);
            REQUIRE(tensor_arr.dim_names().has_value());
            CHECK_EQ(tensor_arr.dim_names()->size(), 2);
            CHECK_EQ((*tensor_arr.dim_names())[0], "rows");
            CHECK_EQ((*tensor_arr.dim_names())[1], "cols");
        }

        TEST_CASE("constructor_with_column_major")
        {
            std::vector<int64_t> shape = {2, 3};
            std::vector<double> flat_data(12);  // 2 tensors
            std::iota(flat_data.begin(), flat_data.end(), 1.0);

            primitive_array<double> flat_arr(flat_data);
            fixed_shape_tensor_array tensor_arr(
                shape,
                array(std::move(flat_arr)),
                false,
                std::nullopt,  // no dim names
                true  // column major
            );

            CHECK(tensor_arr.is_column_major());
        }

        TEST_CASE("constructor_with_validity_bitmap")
        {
            std::vector<int64_t> shape = {2, 2};
            std::vector<float> flat_data(12);  // 3 tensors
            std::iota(flat_data.begin(), flat_data.end(), 1.0f);

            primitive_array<float> flat_arr(flat_data);
            
            // Create validity bitmap: tensor 0 and 2 valid, tensor 1 null
            validity_bitmap vbitmap(3);
            vbitmap.set(0, true);
            vbitmap.set(1, false);
            vbitmap.set(2, true);

            fixed_shape_tensor_array tensor_arr(
                shape,
                array(std::move(flat_arr)),
                std::move(vbitmap)
            );

            CHECK_EQ(tensor_arr.size(), 3);
            CHECK(tensor_arr[0].has_value());
            CHECK_FALSE(tensor_arr[1].has_value());
            CHECK(tensor_arr[2].has_value());

            // Check values of valid tensors
            auto tensor0 = tensor_arr[0].value();
            CHECK_EQ(tensor0.size(), 4);
            CHECK_NULLABLE_VARIANT_EQ(tensor0[0], 1.0f);
            
            auto tensor2 = tensor_arr[2].value();
            CHECK_EQ(tensor2.size(), 4);
            CHECK_NULLABLE_VARIANT_EQ(tensor2[0], 9.0f);
        }

        TEST_CASE("copy_constructor")
        {
            std::vector<int64_t> shape = {2, 3};
            std::vector<double> flat_data(12);
            std::iota(flat_data.begin(), flat_data.end(), 1.0);

            primitive_array<double> flat_arr(flat_data);
            fixed_shape_tensor_array tensor_arr1(
                shape,
                array(std::move(flat_arr)),
                false
            );

            fixed_shape_tensor_array tensor_arr2(tensor_arr1);

            CHECK_EQ(tensor_arr2.size(), tensor_arr1.size());
            CHECK_EQ(tensor_arr2.shape(), tensor_arr1.shape());
            
            // Verify data was copied
            auto tensor1_0 = tensor_arr1[0].value();
            auto tensor2_0 = tensor_arr2[0].value();
            CHECK_EQ(tensor1_0.size(), tensor2_0.size());
            for (size_t i = 0; i < tensor1_0.size(); ++i)
            {
                CHECK_EQ(tensor1_0[i], tensor2_0[i]);
            }
        }

        TEST_CASE("copy_assignment")
        {
            std::vector<int64_t> shape1 = {2, 2};
            std::vector<double> flat_data1(8);
            std::iota(flat_data1.begin(), flat_data1.end(), 1.0);
            primitive_array<double> flat_arr1(flat_data1);
            fixed_shape_tensor_array tensor_arr1(
                shape1,
                array(std::move(flat_arr1)),
                false
            );

            std::vector<int64_t> shape2 = {3, 3};
            std::vector<double> flat_data2(9);
            std::iota(flat_data2.begin(), flat_data2.end(), 100.0);
            primitive_array<double> flat_arr2(flat_data2);
            fixed_shape_tensor_array tensor_arr2(
                shape2,
                array(std::move(flat_arr2)),
                false
            );

            tensor_arr2 = tensor_arr1;

            CHECK_EQ(tensor_arr2.size(), tensor_arr1.size());
            CHECK_EQ(tensor_arr2.shape(), tensor_arr1.shape());
        }

        TEST_CASE("move_constructor")
        {
            std::vector<int64_t> shape = {2, 3};
            std::vector<double> flat_data(12);
            std::iota(flat_data.begin(), flat_data.end(), 1.0);

            primitive_array<double> flat_arr(flat_data);
            fixed_shape_tensor_array tensor_arr1(
                shape,
                array(std::move(flat_arr)),
                false
            );

            const size_t original_size = tensor_arr1.size();
            const auto original_shape = tensor_arr1.shape();

            fixed_shape_tensor_array tensor_arr2(std::move(tensor_arr1));

            CHECK_EQ(tensor_arr2.size(), original_size);
            CHECK_EQ(tensor_arr2.shape(), original_shape);
        }

        TEST_CASE("iteration")
        {
            std::vector<int64_t> shape = {2, 2};
            std::vector<int32_t> flat_data(12);  // 3 tensors
            std::iota(flat_data.begin(), flat_data.end(), 0);

            primitive_array<int32_t> flat_arr(flat_data);
            fixed_shape_tensor_array tensor_arr(
                shape,
                array(std::move(flat_arr)),
                false
            );

            size_t count = 0;
            for (const auto& nullable_tensor : tensor_arr)
            {
                CHECK(nullable_tensor.has_value());
                const auto& tensor = nullable_tensor.value();
                CHECK_EQ(tensor.size(), 4);
                ++count;
            }
            CHECK_EQ(count, 3);
        }

        TEST_CASE("raw_flat_array_access")
        {
            std::vector<int64_t> shape = {2, 3};
            std::vector<double> flat_data(12);
            std::iota(flat_data.begin(), flat_data.end(), 1.0);

            primitive_array<double> flat_arr(flat_data);
            fixed_shape_tensor_array tensor_arr(
                shape,
                array(std::move(flat_arr)),
                false
            );

            const array_wrapper* flat_array = tensor_arr.raw_flat_array();
            REQUIRE(flat_array != nullptr);
            CHECK_EQ(flat_array->get_arrow_proxy().length(), 12);
        }

        TEST_CASE("metadata_preservation")
        {
            SUBCASE("simple metadata")
            {
                std::vector<int64_t> shape = {3, 4, 5};
                std::vector<float> flat_data(60);  // 1 tensor
                std::iota(flat_data.begin(), flat_data.end(), 1.0f);

                primitive_array<float> flat_arr(flat_data);
                fixed_shape_tensor_array tensor_arr(
                    shape,
                    array(std::move(flat_arr)),
                    false
                );

                // Verify metadata is accessible
                CHECK_EQ(tensor_arr.shape().size(), 3);
                CHECK_EQ(tensor_arr.shape()[0], 3);
                CHECK_EQ(tensor_arr.shape()[1], 4);
                CHECK_EQ(tensor_arr.shape()[2], 5);
            }

            SUBCASE("with dimension names and column major")
            {
                std::vector<int64_t> shape = {2, 3, 4};
                std::vector<std::string> dim_names = {"x", "y", "z"};
                std::vector<double> flat_data(24);
                std::iota(flat_data.begin(), flat_data.end(), 1.0);

                primitive_array<double> flat_arr(flat_data);
                fixed_shape_tensor_array tensor_arr(
                    shape,
                    array(std::move(flat_arr)),
                    false,
                    dim_names,
                    true  // column major
                );

                CHECK(tensor_arr.is_column_major());
                REQUIRE(tensor_arr.dim_names().has_value());
                CHECK_EQ(tensor_arr.dim_names()->size(), 3);
                CHECK_EQ((*tensor_arr.dim_names())[0], "x");
                CHECK_EQ((*tensor_arr.dim_names())[1], "y");
                CHECK_EQ((*tensor_arr.dim_names())[2], "z");
            }
        }

        TEST_CASE("different_data_types")
        {
            SUBCASE("int8_t")
            {
                std::vector<int64_t> shape = {3, 3};
                std::vector<int8_t> flat_data(9);
                std::iota(flat_data.begin(), flat_data.end(), static_cast<int8_t>(1));

                primitive_array<int8_t> flat_arr(flat_data);
                fixed_shape_tensor_array tensor_arr(
                    shape,
                    array(std::move(flat_arr)),
                    false
                );

                CHECK_EQ(tensor_arr.size(), 1);
                auto tensor = tensor_arr[0].value();
                CHECK_EQ(tensor.size(), 9);
            }

            SUBCASE("uint32_t")
            {
                std::vector<int64_t> shape = {2, 4};
                std::vector<uint32_t> flat_data(16);
                std::iota(flat_data.begin(), flat_data.end(), 100u);

                primitive_array<uint32_t> flat_arr(flat_data);
                fixed_shape_tensor_array tensor_arr(
                    shape,
                    array(std::move(flat_arr)),
                    false
                );

                CHECK_EQ(tensor_arr.size(), 2);
            }

            SUBCASE("float16")
            {
                std::vector<int64_t> shape = {2, 2};
                std::vector<float16_t> flat_data(8);
                for (size_t i = 0; i < flat_data.size(); ++i)
                {
                    flat_data[i] = static_cast<float16_t>(i);
                }

                primitive_array<float16_t> flat_arr(flat_data);
                fixed_shape_tensor_array tensor_arr(
                    shape,
                    array(std::move(flat_arr)),
                    false
                );

                CHECK_EQ(tensor_arr.size(), 2);
            }
        }

        TEST_CASE("edge_cases")
        {
            SUBCASE("single_element_tensors")
            {
                std::vector<int64_t> shape = {1};
                std::vector<double> flat_data = {1.0, 2.0, 3.0};

                primitive_array<double> flat_arr(flat_data);
                fixed_shape_tensor_array tensor_arr(
                    shape,
                    array(std::move(flat_arr)),
                    false
                );

                CHECK_EQ(tensor_arr.size(), 3);
                CHECK_EQ(tensor_arr[0].value().size(), 1);
                CHECK_EQ(tensor_arr[1].value().size(), 1);
                CHECK_EQ(tensor_arr[2].value().size(), 1);
            }

            SUBCASE("large_tensors")
            {
                std::vector<int64_t> shape = {10, 10, 10};
                std::vector<double> flat_data(1000);  // 1 tensor
                std::iota(flat_data.begin(), flat_data.end(), 1.0);

                primitive_array<double> flat_arr(flat_data);
                fixed_shape_tensor_array tensor_arr(
                    shape,
                    array(std::move(flat_arr)),
                    false
                );

                CHECK_EQ(tensor_arr.size(), 1);
                auto tensor = tensor_arr[0].value();
                CHECK_EQ(tensor.size(), 1000);
            }
        }

        TEST_CASE("const_correctness")
        {
            std::vector<int64_t> shape = {2, 3};
            std::vector<double> flat_data(12);
            std::iota(flat_data.begin(), flat_data.end(), 1.0);

            primitive_array<double> flat_arr(flat_data);
            const fixed_shape_tensor_array tensor_arr(
                shape,
                array(std::move(flat_arr)),
                false
            );

            // These should work on const array
            CHECK_EQ(tensor_arr.size(), 2);
            CHECK_EQ(tensor_arr.shape().size(), 2);
            CHECK_FALSE(tensor_arr.is_column_major());
            
            const array_wrapper* flat_array = tensor_arr.raw_flat_array();
            CHECK(flat_array != nullptr);

            // Iteration on const
            size_t count = 0;
            for (const auto& tensor : tensor_arr)
            {
                (void)tensor;
                ++count;
            }
            CHECK_EQ(count, 2);
        }
    }
}
