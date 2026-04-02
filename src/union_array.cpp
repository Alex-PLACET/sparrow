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

#include "sparrow/union_array.hpp"

#include "sparrow/array.hpp"
#include "sparrow/debug/copy_tracker.hpp"
#include "sparrow/layout/array_registry.hpp"
#include "sparrow/utils/metadata.hpp"
#include "sparrow/utils/repeat_container.hpp"

namespace sparrow
{
    namespace
    {
        template <class ARRAY>
        constexpr bool supports_union_insert_v = is_nullable_v<typename ARRAY::value_type>
                                                 && requires(
                                                     ARRAY& array,
                                                     typename ARRAY::const_iterator pos,
                                                     const typename ARRAY::value_type& typed_value,
                                                     std::size_t count
                                                 ) { array.insert(pos, typed_value, count); };

        template <class ARRAY>
        constexpr bool supports_union_erase_v = requires(
            ARRAY& array,
            typename ARRAY::const_iterator first,
            typename ARRAY::const_iterator last
        ) { array.erase(first, last); };
    }

    namespace detail
    {
        void validate_union_child_insert_value(const array& child, const array_traits::value_type& value)
        {
            using value_variant_type = typename array_traits::value_type::base_type;

            child.visit(
                [&](const auto& child_impl)
                {
                    using child_array_type = std::decay_t<decltype(child_impl)>;
                    if constexpr (supports_union_insert_v<child_array_type>)
                    {
                        bool matched = false;
                        std::visit(
                            [&](const auto& typed_value)
                            {
                                if constexpr (std::same_as<std::decay_t<decltype(typed_value)>, typename child_array_type::value_type>)
                                {
                                    matched = true;
                                }
                            },
                            static_cast<const value_variant_type&>(value)
                        );

                        if (!matched)
                        {
                            throw std::invalid_argument(
                                "union_array value type does not match the selected child array"
                            );
                        }
                    }
                    else
                    {
                        throw std::runtime_error(
                            "union_array mutability requires child arrays that support insert(value, count)"
                        );
                    }
                }
            );
        }

        void validate_union_child_erase(const array& child)
        {
            child.visit(
                [](const auto& child_impl)
                {
                    using child_array_type = std::decay_t<decltype(child_impl)>;
                    if constexpr (!supports_union_erase_v<child_array_type>)
                    {
                        throw std::runtime_error(
                            "union_array mutability requires child arrays that support erase(first, last)"
                        );
                    }
                }
            );
        }

        void
        insert_union_child_value(array& child, std::size_t pos, const array_traits::value_type& value, std::size_t count)
        {
            using value_variant_type = typename array_traits::value_type::base_type;

            child.visit(
                [&](const auto& child_impl)
                {
                    using child_array_type = std::decay_t<decltype(child_impl)>;
                    if constexpr (supports_union_insert_v<child_array_type>)
                    {
                        bool inserted = false;
                        std::visit(
                            [&](const auto& typed_value)
                            {
                                if constexpr (std::same_as<std::decay_t<decltype(typed_value)>, typename child_array_type::value_type>)
                                {
                                    auto& mutable_child = const_cast<child_array_type&>(child_impl);
                                    auto insert_pos = std::next(
                                        mutable_child.cbegin(),
                                        static_cast<std::ptrdiff_t>(pos)
                                    );
                                    mutable_child.insert(insert_pos, typed_value, count);
                                    inserted = true;
                                }
                            },
                            static_cast<const value_variant_type&>(value)
                        );

                        if (!inserted)
                        {
                            throw std::invalid_argument(
                                "union_array value type does not match the selected child array"
                            );
                        }
                    }
                    else
                    {
                        throw std::runtime_error(
                            "union_array mutability requires child arrays that support insert(value, count)"
                        );
                    }
                }
            );
        }

        auto make_union_child_default_value(const array& child, bool is_valid) -> array_traits::value_type
        {
            return child.visit(
                [is_valid](const auto& child_impl) -> array_traits::value_type
                {
                    using child_array_type = std::decay_t<decltype(child_impl)>;
                    if constexpr (is_nullable_v<typename child_array_type::value_type>)
                    {
                        using child_inner_value_type = typename child_array_type::inner_value_type;
                        using child_value_type = typename child_array_type::value_type;
                        return array_traits::value_type(
                            child_value_type(child_inner_value_type{}, static_cast<bool>(is_valid))
                        );
                    }
                    else
                    {
                        throw std::runtime_error(
                            "union_array sparse mutation does not support union child arrays"
                        );
                    }
                }
            );
        }
    }

    namespace copy_tracker
    {
        template <>
        SPARROW_API std::string key<dense_union_array>()
        {
            return "dense_union_array";
        }

        template <>
        SPARROW_API std::string key<sparse_union_array>()
        {
            return "sparse_union_array";
        }
    }

    /************************************
     * dense_union_array implementation *
     ************************************/

#ifdef __GNUC__
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wcast-align"
#endif

    dense_union_array::dense_union_array(arrow_proxy proxy)
        : base_type(std::move(proxy))
        , p_offsets(make_offsets())
    {
    }

    dense_union_array::dense_union_array(const dense_union_array& rhs)
        : dense_union_array(rhs.m_proxy)
    {
        copy_tracker::increase(copy_tracker::key<dense_union_array>());
    }

    dense_union_array& dense_union_array::operator=(const dense_union_array& rhs)
    {
        copy_tracker::increase(copy_tracker::key<dense_union_array>());
        if (this != &rhs)
        {
            base_type::operator=(rhs);
            p_offsets = make_offsets();
        }
        return *this;
    }

#ifdef __GNUC__
#    pragma GCC diagnostic pop
#endif

    std::size_t dense_union_array::element_offset(std::size_t i) const
    {
        return static_cast<std::size_t>(p_offsets[i]);
    }

    /*************************************
     * sparse_union_array implementation *
     *************************************/

    sparse_union_array::sparse_union_array(arrow_proxy proxy)
        : base_type(std::move(proxy))
    {
    }

    sparse_union_array::sparse_union_array(const sparse_union_array& rhs)
        : base_type(rhs)
    {
        copy_tracker::increase(copy_tracker::key<sparse_union_array>());
    }

    sparse_union_array& sparse_union_array::operator=(const sparse_union_array& rhs)
    {
        copy_tracker::increase(copy_tracker::key<sparse_union_array>());
        if (this != &rhs)
        {
            base_type::operator=(rhs);
        }
        return *this;
    }

    std::size_t sparse_union_array::element_offset(std::size_t i) const
    {
        return i + m_proxy.offset();
    }
}
