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

#pragma once

// tiM : std::chrono::months
// tiD : sparrow::day_time_interval
// tin : sparrow::month_day_nanoseconds_interval

#include "sparrow/layout/array_base.hpp"
#include "sparrow/layout/array_bitmap_base.hpp"
#include "sparrow/layout/fixed_width_binary_layout/fixed_width_binary_array.hpp"
#include "sparrow/layout/temporal/interval_concepts.hpp"
#include "sparrow/utils/mp_utils.hpp"

namespace sparrow
{
    template <interval_type T>
    class interval_array;

    template <interval_type T>
    struct array_inner_types<interval_array<T>> : array_inner_types_base
    {
        using self_type = interval_array<T>;

        using inner_value_type = T;
        using inner_reference = T&;
        using inner_const_reference = const T&;
        using pointer = inner_value_type*;
        using const_pointer = const inner_value_type*;

        using value_iterator = pointer_iterator<pointer>;
        using const_value_iterator = pointer_iterator<const_pointer>;
        using bitmap_const_reference = bitmap_type::const_reference;
        using const_reference = nullable<inner_const_reference, bitmap_const_reference>;
        using iterator_tag = std::random_access_iterator_tag;
    };

    template <interval_type T>
    struct is_interval_array : std::false_type
    {
    };

    template <interval_type T>
    struct is_interval_array<interval_array<T>> : std::true_type
    {
    };

    template <interval_type T>
    constexpr bool is_interval_array_v = is_interval_array<T>::value;

    template <interval_type T>
    class interval_array final : public mutable_array_bitmap_base<interval_array<T>>
    {
    public:

        using self_type = interval_array;
        using base_type = mutable_array_bitmap_base<self_type>;

        using inner_types = array_inner_types<self_type>;
        using inner_value_type = typename inner_types::inner_value_type;
        using inner_reference = typename inner_types::inner_reference;
        using inner_const_reference = typename inner_types::inner_const_reference;

        using bitmap_type = typename base_type::bitmap_type;
        using bitmap_reference = typename base_type::bitmap_reference;
        using bitmap_const_reference = typename base_type::bitmap_const_reference;
        using bitmap_iterator = typename base_type::bitmap_iterator;
        using const_bitmap_iterator = typename base_type::const_bitmap_iterator;
        using bitmap_range = typename base_type::bitmap_range;
        using const_bitmap_range = typename base_type::const_bitmap_range;

        using value_type = nullable<inner_value_type>;
        using reference = nullable<inner_reference, bitmap_reference>;
        using const_reference = nullable<inner_const_reference, bitmap_const_reference>;

        using pointer = typename inner_types::pointer;
        using const_pointer = typename inner_types::const_pointer;

        using size_type = typename base_type::size_type;
        using difference_type = typename base_type::difference_type;
        using iterator_tag = typename base_type::iterator_tag;

        using value_iterator = typename base_type::value_iterator;
        using const_value_iterator = typename base_type::const_value_iterator;

        using iterator = typename base_type::iterator;
        using const_iterator = typename base_type::const_iterator;

        using buffer_inner_value_type = T::rep;

        explicit interval_array(arrow_proxy);

        /**
         * Construct a duration array with the passed range of values and an optional bitmap.
         *
         * The first argument can be any range of values as long as its value type is convertible
         * to \c T .
         * The second argument can be:
         * - a bitmap range, i.e. a range of boolean-like values indicating the non-missing values.
         *   The bitmap range and the value range must have the same size.
         * ```cpp
         * TODO
         * ```
         * - a range of indices indicating the missing values.
         * ```cpp
         * std::vector<std::size_t> false_pos  { 3, 8 };
         * primitive_array<int> pr(input_values, a_bitmap);
         * ```
         * - omitted: this is equivalent as passing a bitmap range full of \c true.
         * ```cpp
         * primitive_array<int> pr(input_values);
         * ```
         */
        template <class... Args>
            requires(mpl::excludes_copy_and_move_ctor_v<interval_array, Args...>)
        explicit interval_array(Args&&... args)
            : base_type(create_proxy(std::forward<Args>(args)...))
            , m_values_layout(create_values_layout(this->get_arrow_proxy()))
        {
        }

        interval_array(
            std::initializer_list<inner_value_type> init,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<std::string_view> metadata = std::nullopt
        )
            : base_type(create_proxy(init, std::move(name), std::move(metadata)))
            , m_values_layout(create_values_layout(this->get_arrow_proxy()))
        {
        }

    private:

        using values_layout = fixed_width_binary_array;

        [[nodiscard]] inner_reference value(size_type i);
        [[nodiscard]] inner_const_reference value(size_type i) const;

        [[nodiscard]] value_iterator value_begin();
        [[nodiscard]] value_iterator value_end();

        [[nodiscard]] const_value_iterator value_cbegin() const;
        [[nodiscard]] const_value_iterator value_cend() const;

        static arrow_proxy create_proxy(
            size_type n,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<std::string_view> metadata = std::nullopt
        );

        template <validity_bitmap_input R = validity_bitmap>
        static auto create_proxy(
            u8_buffer<buffer_inner_value_type>&& data_buffer,
            R&& bitmaps = validity_bitmap{},
            std::optional<std::string_view> name = std::nullopt,
            std::optional<std::string_view> metadata = std::nullopt
        ) -> arrow_proxy;

        // range of values (no missing values)
        template <std::ranges::input_range R>
            requires std::convertible_to<std::ranges::range_value_t<R>, T>
        static auto create_proxy(
            R&& range,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<std::string_view> metadata = std::nullopt
        ) -> arrow_proxy;

        template <typename U>
            requires std::convertible_to<U, T>
        static arrow_proxy create_proxy(
            size_type n,
            const U& value = U{},
            std::optional<std::string_view> name = std::nullopt,
            std::optional<std::string_view> metadata = std::nullopt
        );

        // range of values, validity_bitmap_input
        template <std::ranges::input_range VALUE_RANGE, validity_bitmap_input VALIDITY_RANGE>
            requires(std::convertible_to<std::ranges::range_value_t<VALUE_RANGE>, T>)
        static arrow_proxy create_proxy(
            VALUE_RANGE&&,
            VALIDITY_RANGE&&,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<std::string_view> metadata = std::nullopt
        );

        // range of nullable values
        template <std::ranges::input_range R>
            requires std::is_same_v<std::ranges::range_value_t<R>, nullable<T>>
        static arrow_proxy create_proxy(
            R&&,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<std::string_view> metadata = std::nullopt
        );

        // Modifiers

        void resize_values(size_type new_length, inner_value_type value);

        value_iterator insert_value(const_value_iterator pos, inner_value_type value, size_type count);

        template <mpl::iterator_of_type<typename interval_array<T>::inner_value_type> InputIt>
        auto insert_values(const_value_iterator pos, InputIt first, InputIt last) -> value_iterator
        {
            const auto input_range = std::ranges::subrange(first, last);
            const auto values = input_range
                                | std::views::transform(
                                    [](const auto& v)
                                    {
                                        return v.count();
                                    }
                                );
            const auto idx = std::distance(value_cbegin(), pos);
            const auto values_layout_pos = m_values_layout.value_cbegin() + idx;
            const auto ret_pos = m_values_layout.insert_values(values_layout_pos, values.begin(), values.end());
            const auto distance = std::distance(m_values_layout.value_begin(), ret_pos);
            detail::array_access::get_arrow_proxy(m_values_layout).update_buffers();
            return value_begin() + distance;
        }

        value_iterator erase_values(const_value_iterator pos, size_type count);

        [[nodiscard]] static values_layout create_values_layout(arrow_proxy& proxy);

        values_layout m_values_layout;

        static constexpr size_type DATA_BUFFER_INDEX = 1;
        friend base_type;
        friend base_type::base_type;
        friend base_type::base_type::base_type;
    };

    template <interval_type T>
    auto interval_array<T>::create_values_layout(arrow_proxy& proxy) -> values_layout
    {
        arrow_proxy arr_proxy{&proxy.array(), &proxy.schema()};
        return values_layout{std::move(arr_proxy)};
    }

    template <interval_type T>
    interval_array<T>::interval_array(arrow_proxy proxy)
        : base_type(std::move(proxy))
        , m_values_layout(create_values_layout(proxy))
    {
    }

    template <interval_type T>
    template <validity_bitmap_input R>
    auto interval_array<T>::create_proxy(
        u8_buffer<buffer_inner_value_type>&& data_buffer,
        R&& bitmap_input,
        std::optional<std::string_view> name,
        std::optional<std::string_view> metadata
    ) -> arrow_proxy
    {
        const auto size = data_buffer.size();
        validity_bitmap bitmap = ensure_validity_bitmap(size, std::forward<R>(bitmap_input));
        const auto null_count = bitmap.null_count();
        // create arrow schema and array
        ArrowSchema schema = make_arrow_schema(
            data_type_to_format(arrow_traits<T>::type_id),  // format
            std::move(name),                                // name
            std::move(metadata),                            // metadata
            std::nullopt,                                   // flags
            0,                                              // n_children
            nullptr,                                        // children
            nullptr                                         // dictionary
        );

        std::vector<buffer<uint8_t>> buffers(2);
        buffers[0] = std::move(bitmap).extract_storage();
        buffers[1] = std::move(data_buffer).extract_storage();

        // create arrow array
        ArrowArray arr = make_arrow_array(
            static_cast<std::int64_t>(size),  // length
            static_cast<int64_t>(null_count),
            0,  // offset
            std::move(buffers),
            0,        // n_children
            nullptr,  // children
            nullptr   // dictionary
        );
        return arrow_proxy(std::move(arr), std::move(schema));
    }

    template <interval_type T>
    template <std::ranges::input_range VALUE_RANGE, validity_bitmap_input VALIDITY_RANGE>
        requires(std::convertible_to<std::ranges::range_value_t<VALUE_RANGE>, T>)
    arrow_proxy interval_array<T>::create_proxy(
        VALUE_RANGE&& values,
        VALIDITY_RANGE&& validity_input,
        std::optional<std::string_view> name,
        std::optional<std::string_view> metadata
    )
    {
        const auto range = values
                           | std::views::transform(
                               [](const auto& v)
                               {
                                   return v.count();
                               }
                           );
        u8_buffer<buffer_inner_value_type> data_buffer(range);
        return create_proxy(
            std::move(data_buffer),
            std::forward<VALIDITY_RANGE>(validity_input),
            std::move(name),
            std::move(metadata)
        );
    }

    template <interval_type T>
    template <typename U>
        requires std::convertible_to<U, T>
    arrow_proxy interval_array<T>::create_proxy(
        size_type n,
        const U& value,
        std::optional<std::string_view> name,
        std::optional<std::string_view> metadata
    )
    {
        u8_buffer<buffer_inner_value_type> data_buffer(n, to_days_since_the_UNIX_epoch(value));
        return create_proxy(std::move(data_buffer), std::move(name), std::move(metadata));
    }

    template <interval_type T>
    template <std::ranges::input_range R>
        requires std::convertible_to<std::ranges::range_value_t<R>, T>
    arrow_proxy interval_array<T>::create_proxy(
        R&& range,
        std::optional<std::string_view> name,
        std::optional<std::string_view> metadata
    )
    {
        const std::size_t n = range_size(range);
        const auto iota = std::ranges::iota_view{std::size_t(0), n};
        std::ranges::transform_view iota_to_is_non_missing(
            iota,
            [](std::size_t)
            {
                return true;
            }
        );
        return self_type::create_proxy(
            std::forward<R>(range),
            std::move(iota_to_is_non_missing),
            std::move(name),
            std::move(metadata)
        );
    }

    // range of nullable values
    template <interval_type T>
    template <std::ranges::input_range R>
        requires std::is_same_v<std::ranges::range_value_t<R>, nullable<T>>
    arrow_proxy interval_array<T>::create_proxy(
        R&& range,
        std::optional<std::string_view> name,
        std::optional<std::string_view> metadata
    )
    {  // split into values and is_non_null ranges
        auto values = range
                      | std::views::transform(
                          [](const auto& v)
                          {
                              return v.get();
                          }
                      );
        auto is_non_null = range
                           | std::views::transform(
                               [](const auto& v)
                               {
                                   return v.has_value();
                               }
                           );
        return self_type::create_proxy(values, is_non_null, std::move(name), std::move(metadata));
    }

    template <interval_type T>
    auto interval_array<T>::value(size_type i) -> inner_reference
    {
        SPARROW_ASSERT_TRUE(i < this->size());
        return reinterpret_cast<inner_reference>(m_values_layout.value(i));
    }

    template <interval_type T>
    auto interval_array<T>::value(size_type i) const -> inner_const_reference
    {
        SPARROW_ASSERT_TRUE(i < this->size());
        return reinterpret_cast<inner_const_reference>(m_values_layout.value(i));
    }

    template <interval_type T>
    auto interval_array<T>::value_begin() -> value_iterator
    {
        auto* value_ptr = m_values_layout.data(0);
        return value_iterator{reinterpret_cast<pointer>(value_ptr)};
    }

    template <interval_type T>
    auto interval_array<T>::value_end() -> value_iterator
    {
        return sparrow::next(value_begin(), this->size());
    }

    template <interval_type T>
    auto interval_array<T>::value_cbegin() const -> const_value_iterator
    {
        const auto* value_ptr = m_values_layout.data(0);
        return const_value_iterator{reinterpret_cast<const_pointer>(value_ptr)};
    }

    template <interval_type T>
    auto interval_array<T>::value_cend() const -> const_value_iterator
    {
        return sparrow::next(value_cbegin(), this->size());
    }

    template <interval_type T>
    void interval_array<T>::resize_values(size_type new_length, inner_value_type value)
    {
        const auto value_to_insert = value.count();
        m_values_layout.resize_values(new_length, value_to_insert);
        detail::array_access::get_arrow_proxy(m_values_layout).update_buffers();
    }

    template <interval_type T>
    auto interval_array<T>::insert_value(const_value_iterator pos, inner_value_type value, size_type count)
        -> value_iterator
    {
        const auto idx = std::distance(value_cbegin(), pos);
        const auto values_layout_pos = m_values_layout.value_cbegin() + idx;
        const auto value_to_insert = value.count();
        const auto values_layout_ret = m_values_layout.insert_value(values_layout_pos, value_to_insert, count);
        detail::array_access::get_arrow_proxy(m_values_layout).update_buffers();
        return value_iterator(
            sparrow::next(value_begin(), std::distance(m_values_layout.value_begin(), values_layout_ret))
        );
    }

    template <interval_type T>
    auto interval_array<T>::erase_values(const_value_iterator pos, size_type count) -> value_iterator
    {
        const auto idx = std::distance(value_cbegin(), pos);
        const auto values_layout_pos = m_values_layout.value_cbegin() + idx;
        const auto values_layout_ret = m_values_layout.erase_values(values_layout_pos, count);
        return value_iterator(
            sparrow::next(value_begin(), std::distance(m_values_layout.value_begin(), values_layout_ret))
        );
    }
}
