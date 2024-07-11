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

#include <algorithm>
#include <ranges>
#include <vector>

#include "sparrow/details/3rdparty/value_ptr_lite.hpp"
#include "sparrow/mp_utils.hpp"

namespace sparrow
{
    /**
     * Get a raw pointer from a smart pointer, a range, an object or a pointer.
     *
     * @tparam T The type of the pointer to obtain.
     * @tparam U The type of the variable.
     * @param var The variable.
     * @return A raw pointer.
     *          If the variable is a smart pointer, the pointer is obtained by calling get().
     *          If the variable is a range, the pointer is obtained by calling data().
     *          If the variable is a pointer, the pointer is returned as is.
     *          If the variable is an object, the pointer is returned by calling the address-of operator.
     */
    template <typename T, typename U>
    T* get_raw_ptr(U& var)
    {
        if constexpr (std::is_pointer_v<U>)
        {
            return var;
        }
        else if constexpr (mpl::has_element_type<U>)
        {
            if constexpr (mpl::smart_ptr<U> || std::derived_from<U, std::shared_ptr<typename U::element_type>>
                          || mpl::is_type_instance_of_v<U, nonstd::value_ptr>)
            {
                if constexpr (std::ranges::input_range<typename U::element_type>)
                {
                    return std::ranges::data(*var.get());
                }
                else if constexpr (std::same_as<typename U::element_type, T> || std::same_as<T, void>)
                {
                    return var.get();
                }
            }
        }
        else if constexpr (std::ranges::input_range<U>)
        {
            return std::ranges::data(var);
        }
        else if constexpr (std::same_as<T, U> || std::same_as<T, void>)
        {
            return &var;
        }
        else
        {
            static_assert(mpl::dependent_false<T, U>::value, "get_raw_ptr: unsupported type.");
            mpl::unreachable();
        }
    }

    /**
     * Create a vector of pointers to elements from a range.
     *
     * @tparam T The type of the pointers.
     * @tparam Range The range type.
     * @tparam Allocator The allocator type.
     * @param range The range.
     * @return A vector of pointers.
     */
    template <class T, std::ranges::input_range Range, template <typename> class Allocator = std::allocator>
    std::vector<T*, Allocator<T*>> to_raw_ptr_vec(Range& range)
    {
        std::vector<T*, Allocator<T*>> raw_ptr_vec;
        raw_ptr_vec.reserve(range.size());
        std::ranges::transform(
            range,
            std::back_inserter(raw_ptr_vec),
            [](auto& elem) -> T*
            {
                return get_raw_ptr<T>(elem);
            }
        );
        return raw_ptr_vec;
    }

    /**
     * Create a vector of pointers to elements of a tuple.
     * Types of the tuple can be nonstd::value_ptr, smart pointers, ranges, objects or pointers.
     * The type of the elements can be different.
     * Reinterpret cast is used to convert the pointers to the desired type.
     *
     * @tparam T The type of the pointers.
     * @tparam Tuple The tuple type.
     * @tparam Allocator The allocator type.
     * @param tuple The tuple.
     * @return A vector of pointers.
     */
    template <class T, class Tuple, template <typename> class Allocator = std::allocator>
        requires mpl::is_type_instance_of_v<Tuple, std::tuple>
    std::vector<T*, Allocator<T*>> to_raw_ptr_vec(Tuple& tuple)
    {
        std::vector<T*, Allocator<T*>> raw_ptr_vec;
        raw_ptr_vec.reserve(std::tuple_size_v<Tuple>);
        std::apply(
            [&raw_ptr_vec](auto&&... args)
            {
                (raw_ptr_vec.push_back(get_raw_ptr<T>(args)), ...);
            },
            tuple
        );
        return raw_ptr_vec;
    }

    template <class Ptr>
    using value_ptr_from = nonstd::value_ptr<
        typename Ptr::element_type,
        nonstd::detail::default_clone<typename Ptr::element_type>,
        typename Ptr::deleter_type>;

    /**
     * Transforms a range of unique pointers to a vector of nonstd::value_ptr.
     *
     * @tparam Input A range of unique_ptr.
     * @param input The input range.
     * @return A vector of nonstd::value_ptr.
     */
    template <std::ranges::input_range Input>
        requires mpl::unique_ptr<std::ranges::range_value_t<Input>>
    std::vector<value_ptr_from<std::ranges::range_value_t<Input>>>
    range_of_unique_ptr_to_vec_of_value_ptr(Input& input)
    {
        std::vector<value_ptr_from<std::ranges::range_value_t<Input>>> values_ptrs;
        values_ptrs.reserve(std::ranges::size(input));
        std::ranges::transform(
            input,
            std::back_inserter(values_ptrs),
            [](auto& child)
            {
                return value_ptr_from<std::ranges::range_value_t<Input>>{child.release()};
            }
        );
        return values_ptrs;
    }
}