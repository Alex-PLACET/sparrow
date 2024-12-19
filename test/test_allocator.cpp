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

#include <memory>
#include <memory_resource>
#include <numeric>
#include <vector>

#include <catch2/catch_all.hpp>

#include "sparrow/buffer/allocator.hpp"

#if defined(__APPLE__)
using value_semantic_id = std::tuple<std::allocator<int>>;
using allocate_id = std::tuple<std::allocator<int>>;
#else
using value_semantic_id = std::tuple<std::allocator<int>, std::pmr::polymorphic_allocator<int>>;
using allocate_id = std::tuple<std::allocator<int>, std::pmr::polymorphic_allocator<int>>;
#endif

TEMPLATE_LIST_TEST_CASE("any_allocator", "", value_semantic_id)
{
    SECTION("value semantic")
    {
        SECTION("constructor")
        {
            {
                sparrow::any_allocator<int> a;
            }
            {
                TestType alloc;
                sparrow::any_allocator<typename TestType::value_type> a(alloc);
            }
        }

        SECTION("copy constructor")
        {
            using value_type = typename TestType::value_type;

            TestType alloc;
            sparrow::any_allocator<value_type> a(alloc);
            sparrow::any_allocator<value_type> b(a);
            CHECK(a == b);

            auto d = b.select_on_container_copy_construction();
            CHECK(d == b);
        }

        SECTION("move constructor")
        {
            using value_type = typename TestType::value_type;

            TestType alloc;
            sparrow::any_allocator<value_type> a(alloc);
            sparrow::any_allocator<value_type> aref(a);
            sparrow::any_allocator<value_type> b(std::move(a));
            CHECK(b == aref);
        }
    }
}

TEMPLATE_LIST_TEST_CASE("allocate / deallocate", "", allocate_id)
{
    using value_type = typename TestType::value_type;

    constexpr std::size_t n = 100;
    std::vector<value_type> ref(n);
    std::iota(ref.begin(), ref.end(), value_type());

    TestType alloc;
    sparrow::any_allocator<value_type> a(alloc);
    value_type* buf = a.allocate(n);
    std::uninitialized_copy(ref.cbegin(), ref.cend(), buf);
    CHECK(*buf == ref[0]);
    CHECK(*(buf + n - 1) == ref.back());
    a.deallocate(buf, n);
}
