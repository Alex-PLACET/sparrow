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

#pragma once

#include <cstddef>
#include <ranges>
#include <tuple>
#include <type_traits>

#include "sparrow/c_interface.hpp"
#include "sparrow/mp_utils.hpp"

namespace sparrow
{
    /// A concept that checks if a type is an ArrowArray or an ArrowSchema.
    template <typename T>
    concept any_arrow_array = std::is_same_v<T, ArrowArray> || std::is_same_v<T, ArrowSchema>;

    /// A concept that checks if a type is a valid Arrow Array buffers or children variable.
    template <class T>
    concept arrow_array_buffers_or_children = std::ranges::sized_range<T>
                                              || mpl::is_type_instance_of_v<T, std::tuple>
                                              || std::is_same_v<T, std::nullptr_t> || std::is_pointer_v<T>;

    template <class T>
    constexpr int64_t get_size(const T& value)
        requires arrow_array_buffers_or_children<T>
    {
        if constexpr (std::ranges::sized_range<T>)
        {
            return static_cast<int64_t>(std::ranges::size(value));
        }
        else if constexpr (mpl::is_type_instance_of_v<T, std::tuple>)
        {
            return std::tuple_size_v<T>;
        }
        else
        {
            return 0;
        }
    }
}
