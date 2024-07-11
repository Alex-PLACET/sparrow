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

#include "sparrow/any_data_utils.hpp"
#include "sparrow/buffer.hpp"

#include "doctest/doctest.h"

TEST_SUITE("any_data utils")
{
    TEST_CASE("get_raw_ptr")
    {
        SUBCASE("int")
        {
            int i = 5;
            auto raw_ptr = sparrow::get_raw_ptr<int>(i);
            CHECK_EQ(*raw_ptr, 5);
        }

        SUBCASE("int*")
        {
            int i = 5;
            int* ptr = &i;
            auto raw_ptr = sparrow::get_raw_ptr<int>(ptr);
            CHECK_EQ(*raw_ptr, 5);
        }

        SUBCASE("std::unique_ptr<int>")
        {
            std::unique_ptr<int> ptr = std::make_unique<int>(5);
            auto raw_ptr = sparrow::get_raw_ptr<int>(ptr);
            CHECK_EQ(*raw_ptr, 5);
        }

        SUBCASE("std::unique_ptr<int[]>")
        {
            std::unique_ptr<int[]> ptr = std::make_unique<int[]>(5);
            auto raw_ptr = sparrow::get_raw_ptr<int>(ptr);
            CHECK_EQ(raw_ptr[0], 0);
        }

        SUBCASE("std::vector<int>")
        {
            std::vector<int> vec = {0, 1, 2, 3, 4, 5};
            auto raw_ptr = sparrow::get_raw_ptr<int>(vec);
            for (size_t i = 0; i < vec.size(); ++i)
            {
                CHECK_EQ(raw_ptr[i], vec[i]);
            }
        }

        SUBCASE("std::unique_ptr<sparrow::buffer<int>>")
        {
            std::unique_ptr<sparrow::buffer<int>> ptr = std::make_unique<sparrow::buffer<int>>(5);
            for (size_t i = 0; i < ptr->size(); ++i)
            {
                ptr->data()[i] = 0;
            }
            auto raw_ptr = sparrow::get_raw_ptr<int>(ptr);
            for (size_t i = 0; i < ptr->size(); ++i)
            {
                CHECK_EQ(raw_ptr[i], 0);
            }
        }
    }

    TEST_CASE("to_raw_ptr_vec")
    {
        SUBCASE("std::vector<std::unique_ptr<int>>")
        {
            std::vector<std::unique_ptr<int>> vec;
            for (int i = 0; i < 5; ++i)
            {
                vec.push_back(std::make_unique<int>(i));
            }
            auto raw_ptr_vec = sparrow::to_raw_ptr_vec<int>(vec);
            for (size_t i = 0; i < vec.size(); ++i)
            {
                CHECK_EQ(*raw_ptr_vec[i], i);
            }
        }

        SUBCASE("std::vector<int>")
        {
            std::vector<int> vec = {0, 1, 2, 3, 4, 5};
            auto raw_ptr_vec = sparrow::to_raw_ptr_vec<int>(vec);
            for (size_t i = 0; i < vec.size(); ++i)
            {
                CHECK_EQ(*raw_ptr_vec[i], vec[i]);
            }
        }

        SUBCASE("std::vector<std::shared_ptr<int>>")
        {
            std::vector<std::shared_ptr<int>> vec;
            for (int i = 0; i < 5; ++i)
            {
                vec.push_back(std::make_shared<int>(i));
            }
            auto raw_ptr_vec = sparrow::to_raw_ptr_vec<int>(vec);
            for (size_t i = 0; i < vec.size(); ++i)
            {
                CHECK_EQ(*raw_ptr_vec[i], i);
            }
        }

        SUBCASE("std::vector<int*>")
        {
            std::vector<int*> vec;
            for (int i = 0; i < 5; ++i)
            {
                vec.emplace_back(new int(i));
            }
            auto raw_ptr_vec = sparrow::to_raw_ptr_vec<int>(vec);
            for (size_t i = 0; i < vec.size(); ++i)
            {
                CHECK_EQ(*raw_ptr_vec[i], i);
            }
        }
    }

    TEST_CASE("range_of_unique_ptr_to_vec_of_value_ptr")
    {
        std::vector<std::unique_ptr<int>> vec;
        for (int i = 0; i < 5; ++i)
        {
            vec.push_back(std::make_unique<int>(i));
        }
        auto value_ptr_vec = sparrow::range_of_unique_ptr_to_vec_of_value_ptr(vec);
        for (size_t i = 0; i < vec.size(); ++i)
        {
            CHECK_EQ(*value_ptr_vec[i], i);
        }
    }
}