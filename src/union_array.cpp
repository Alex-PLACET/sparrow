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

#include <algorithm>
#include <iterator>
#include <limits>
#include <vector>

#include "sparrow/array.hpp"
#include "sparrow/debug/copy_tracker.hpp"
#include "sparrow/null_array.hpp"

namespace sparrow
{
    namespace
    {
        using dynamic_value = array_traits::value_type;
        using rebuild_value = std::pair<std::size_t, dynamic_value>;

        constexpr std::size_t no_child = std::numeric_limits<std::size_t>::max();

        array make_child_snapshot(const array_wrapper& child)
        {
            array view{child.get_arrow_proxy().view()};
            return view.slice(0, view.size());
        }

        array make_empty_child(const array_wrapper& child)
        {
            switch (child.data_type())
            {
                case data_type::LIST:
                case data_type::LARGE_LIST:
                case data_type::LIST_VIEW:
                case data_type::LARGE_LIST_VIEW:
                case data_type::FIXED_SIZED_LIST:
                case data_type::STRUCT:
                case data_type::MAP:
                case data_type::DENSE_UNION:
                case data_type::SPARSE_UNION:
                    return make_child_snapshot(child);
                default:
                    break;
            }

            array view{child.get_arrow_proxy().view()};
            if (view.empty())
            {
                switch (child.data_type())
                {
                    case data_type::DECIMAL32:
                    case data_type::DECIMAL64:
                    case data_type::DECIMAL128:
                    case data_type::DECIMAL256:
                    case data_type::TIMESTAMP_SECONDS:
                    case data_type::TIMESTAMP_MILLISECONDS:
                    case data_type::TIMESTAMP_MICROSECONDS:
                    case data_type::TIMESTAMP_NANOSECONDS:
                        return array_empty_like(view);
                    default:
                        break;
                }
            }
            const auto prototype = view.empty()
                                      ? array_default_value(child)
                                      : array_materialize_element(array_element(child, 0));
            array result = array_make_from_element(prototype);
            auto& result_proxy = detail::array_access::get_arrow_proxy(result);
            const auto& child_proxy = child.get_arrow_proxy();
            result_proxy.set_format(child_proxy.format());
            result_proxy.set_name(child_proxy.name());
            result_proxy.set_metadata(child_proxy.metadata());
            result_proxy.set_flags(child_proxy.flags());
            result.erase(result.cbegin(), result.cend());
            return result;
        }

        dynamic_value make_null_like(const dynamic_value& value)
        {
            return std::visit(
                [](const auto& typed_value) -> dynamic_value
                {
                    using stored_type = std::remove_cvref_t<decltype(typed_value.get())>;
                    return dynamic_value(nullable<stored_type>(stored_type(typed_value.get()), false));
                },
                value
            );
        }

        dynamic_value make_null_value(const array_wrapper& child)
        {
            const auto value = array_default_element_value(child);
            return std::visit(
                [](const auto& typed_value) -> dynamic_value
                {
                    using stored_type = std::decay_t<decltype(typed_value)>;
                    return dynamic_value(nullable<stored_type>(stored_type(typed_value), false));
                },
                value
            );
        }

        struct union_rebuild_payload
        {
            std::vector<std::vector<dynamic_value>> child_values;
            std::vector<std::uint8_t> type_ids;
            std::vector<std::uint32_t> offsets;
        };

        union_rebuild_payload make_dense_rebuild_payload(
            std::span<rebuild_value> values,
            std::span<const std::uint8_t> child_type_ids
        )
        {
            union_rebuild_payload payload;
            payload.child_values.resize(child_type_ids.size());
            payload.type_ids.reserve(values.size());
            payload.offsets.reserve(values.size());

            std::vector<std::size_t> child_value_counts(child_type_ids.size(), 0);
            for (const auto& value : values)
            {
                ++child_value_counts[value.first];
            }
            for (std::size_t child_index = 0; child_index < child_type_ids.size(); ++child_index)
            {
                payload.child_values[child_index].reserve(child_value_counts[child_index]);
            }

            for (std::size_t row = 0; row < values.size(); ++row)
            {
                const auto child_index = values[row].first;
                payload.type_ids.push_back(child_type_ids[child_index]);
                SPARROW_ASSERT_TRUE(
                    payload.child_values[child_index].size() <= std::numeric_limits<std::uint32_t>::max()
                );
                payload.offsets.push_back(static_cast<std::uint32_t>(payload.child_values[child_index].size()));
                payload.child_values[child_index].push_back(std::move(values[row].second));
            }
            return payload;
        }

        template <typename CHILDREN>
        union_rebuild_payload make_sparse_rebuild_payload(
            std::span<rebuild_value> values,
            std::span<const std::uint8_t> child_type_ids,
            const CHILDREN& old_children
        )
        {
            union_rebuild_payload payload;
            payload.child_values.resize(child_type_ids.size());
            payload.type_ids.reserve(values.size());

            std::vector<std::optional<dynamic_value>> null_values(child_type_ids.size());
            for (std::size_t row = 0; row < values.size(); ++row)
            {
                const auto child_index = values[row].first;
                payload.type_ids.push_back(child_type_ids[child_index]);
                if (!null_values[child_index].has_value())
                {
                    null_values[child_index] = make_null_like(values[row].second);
                }
            }

            for (std::size_t child_index = 0; child_index < child_type_ids.size(); ++child_index)
            {
                if (!null_values[child_index].has_value() && child_index < old_children.size()
                    && array_size(*old_children[child_index]) != 0)
                {
                    null_values[child_index] = make_null_like(
                        array_materialize_element(array_element(*old_children[child_index], 0))
                    );
                }
                if (!null_values[child_index].has_value() && child_index < old_children.size())
                {
                    null_values[child_index] = make_null_value(*old_children[child_index]);
                }
                SPARROW_ASSERT_TRUE(null_values[child_index].has_value() || values.empty());

                payload.child_values[child_index].reserve(values.size());
                for (std::size_t row = 0; row < values.size(); ++row)
                {
                    if (values[row].first == child_index)
                    {
                        payload.child_values[child_index].push_back(std::move(values[row].second));
                    }
                    else
                    {
                        payload.child_values[child_index].push_back(*null_values[child_index]);
                    }
                }
            }
            return payload;
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
        , p_offsets(reinterpret_cast<std::int32_t*>(m_proxy.buffers()[1 /*index of offsets*/].data()))
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
            p_offsets = reinterpret_cast<std::int32_t*>(m_proxy.buffers()[1 /*index of offsets*/].data());
        }
        return *this;
    }

#ifdef __GNUC__
#    pragma GCC diagnostic pop
#endif

    std::size_t dense_union_array::element_offset(std::size_t i) const
    {
        return static_cast<std::size_t>(p_offsets[i]) + m_proxy.offset();
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

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::insert(
        const_iterator pos,
        const_reference value,
        size_type count
    ) -> iterator
    {
        if (count == 0)
        {
            return iterator(functor_type{&this->derived_cast()}, static_cast<size_type>(pos - cbegin()));
        }
        return insert_materialized(pos, std::views::single(array_materialize_element(value)), count);
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::insert(
        const_iterator pos,
        const array_traits::value_type& value,
        size_type count
    ) -> iterator
    {
        return insert_materialized(pos, std::views::single(value), count);
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::push_back(const_reference value)
    {
        insert(cend(), value);
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::push_back(const array_traits::value_type& value)
    {
        insert(cend(), value);
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::resize(size_type new_length)
    {
        resize_impl(new_length, array_traits::value_type{});
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::resize(size_type new_length, const_reference value)
    {
        resize_impl(new_length, value);
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::resize(size_type new_length, const array_traits::value_type& value)
    {
        resize_impl(new_length, value);
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::erase(const_iterator pos) -> iterator
    {
        const auto index = static_cast<size_type>(pos - cbegin());
        SPARROW_ASSERT_TRUE(index < size());
        return erase_values(index, 1);
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::erase(const_iterator first, const_iterator last) -> iterator
    {
        const auto first_index = static_cast<size_type>(first - cbegin());
        const auto last_index = static_cast<size_type>(last - cbegin());
        SPARROW_ASSERT_TRUE(first_index <= last_index);
        SPARROW_ASSERT_TRUE(last_index <= size());
        return erase_values(first_index, last_index - first_index);
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::erase_values(size_type first, size_type count) -> iterator
    {
        SPARROW_ASSERT_TRUE(m_proxy.offset() == 0);
        const auto current_size = size();
        SPARROW_ASSERT_TRUE(first <= current_size);
        SPARROW_ASSERT_TRUE(count <= current_size - first);
        if (count == 0)
        {
            return iterator(functor_type{&this->derived_cast()}, first);
        }

        using rebuild_value = std::pair<size_type, dynamic_value>;
        std::vector<rebuild_value> values;
        values.reserve(current_size - count);
        auto retained_values = std::views::iota(size_type{0}, current_size)
                               | std::views::filter(
                                   [first, count](size_type i)
                                   {
                                       return i < first || i >= first + count;
                                   }
                               )
                               | std::views::transform(
                                   [this](size_type i) -> rebuild_value
                                   {
                                       return rebuild_value{
                                           m_type_id_map[p_type_ids[i]],
                                           array_materialize_element((*this)[i])
                                       };
                                   }
                               );
        std::ranges::copy(retained_values, std::back_inserter(values));
        return rebuild_values(std::move(values), first);
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::rebuild_values(
        std::vector<std::pair<size_type, dynamic_value>> values,
        size_type return_index
    ) -> iterator
    {
        const auto old_child_count = m_children.size();
        SPARROW_ASSERT_TRUE(m_child_type_ids.size() == old_child_count);
        std::vector<array> child_templates(old_child_count);
        std::vector<std::uint8_t> child_type_ids = m_child_type_ids;
        std::array<bool, TYPE_ID_MAP_SIZE> used_type_ids{};
        std::vector<data_type> child_data_types;
        child_data_types.reserve(old_child_count);
        for (std::size_t child_index = 0; child_index < old_child_count; ++child_index)
        {
            used_type_ids[child_type_ids[child_index]] = true;
            child_data_types.push_back(m_children[child_index]->data_type());
        }

        std::vector<size_type> value_type_to_child(
            std::variant_size_v<typename dynamic_value::base_type>,
            no_child
        );
        for (std::size_t row = 0; row < values.size(); ++row)
        {
            if (values[row].first != no_child)
            {
                continue;
            }

            auto& cached_child_index = value_type_to_child[values[row].second.index()];
            if (cached_child_index != no_child)
            {
                values[row].first = cached_child_index;
                continue;
            }

            auto child = array_make_from_element(values[row].second);
            const auto child_type = child.data_type();
            const auto existing = std::find(child_data_types.begin(), child_data_types.end(), child_type);
            if (existing != child_data_types.end())
            {
                cached_child_index = static_cast<size_type>(existing - child_data_types.begin());
                values[row].first = cached_child_index;
                continue;
            }

            SPARROW_ASSERT_TRUE(child_data_types.size() < 256);
            std::size_t type_id = 0;
            while (type_id < TYPE_ID_MAP_SIZE && used_type_ids[type_id])
            {
                ++type_id;
            }
            SPARROW_ASSERT_TRUE(type_id < TYPE_ID_MAP_SIZE);
            used_type_ids[type_id] = true;
            values[row].first = child_data_types.size();
            child_data_types.push_back(child_type);
            child_type_ids.push_back(static_cast<std::uint8_t>(type_id));
            cached_child_index = values[row].first;
            child_templates.push_back(std::move(child));
        }

        auto payload = [&]
        {
            if constexpr (is_dense_union_array_v<DERIVED>)
            {
                return make_dense_rebuild_payload(std::span<rebuild_value>{values}, child_type_ids);
            }
            else
            {
                return make_sparse_rebuild_payload(
                    std::span<rebuild_value>{values},
                    child_type_ids,
                    m_children
                );
            }
        }();

        std::vector<array> new_children;
        new_children.reserve(child_data_types.size());
        for (std::size_t child_index = 0; child_index < child_data_types.size(); ++child_index)
        {
            array child = child_index < old_child_count
                              ? make_empty_child(*m_children[child_index])
                              : std::move(child_templates[child_index]);
            auto& values_for_child = payload.child_values[child_index];
            if (child.data_type() == data_type::NA)
            {
                const auto& child_proxy = detail::array_access::get_arrow_proxy(child);
                child = array(null_array(
                    values_for_child.size(),
                    child_proxy.name(),
                    child_proxy.metadata()
                ));
            }
            else
            {
                child.erase(child.cbegin(), child.cend());
                append_values(child, values_for_child);
            }
            new_children.push_back(std::move(child));
        }

        auto metadata = m_proxy.metadata();
        auto replacement = [&]() -> arrow_proxy
        {
            if constexpr (is_dense_union_array_v<DERIVED>)
            {
                return DERIVED::create_proxy(
                    std::move(new_children),
                    typename DERIVED::type_id_buffer_type{payload.type_ids},
                    typename DERIVED::offset_buffer_type{payload.offsets},
                    std::optional<std::vector<std::uint8_t>>(child_type_ids),
                    m_proxy.name(),
                    std::move(metadata)
                );
            }
            else
            {
                return DERIVED::create_proxy(
                    std::move(new_children),
                    typename DERIVED::type_id_buffer_type{payload.type_ids},
                    std::optional<std::vector<std::uint8_t>>(child_type_ids),
                    m_proxy.name(),
                    std::move(metadata)
                );
            }
        }();

        m_proxy = std::move(replacement);
        p_type_ids = reinterpret_cast<std::uint8_t*>(m_proxy.buffers()[0].data());
        m_children = make_children(m_proxy);
        m_child_type_ids = std::move(child_type_ids);
        m_type_id_map = make_type_id_map(m_child_type_ids);
        if constexpr (is_dense_union_array_v<DERIVED>)
        {
            auto& dense = static_cast<dense_union_array&>(this->derived_cast());
            dense.p_offsets = reinterpret_cast<std::int32_t*>(m_proxy.buffers()[1].data());
        }

        return iterator(functor_type{&this->derived_cast()}, return_index);
    }

    template SPARROW_API auto union_array_crtp_base<dense_union_array>::insert(
        const_iterator,
        const_reference,
        size_type
    ) -> iterator;
    template SPARROW_API auto union_array_crtp_base<dense_union_array>::insert(
        const_iterator,
        const array_traits::value_type&,
        size_type
    ) -> iterator;
    template SPARROW_API auto union_array_crtp_base<dense_union_array>::erase(const_iterator) -> iterator;
    template SPARROW_API auto union_array_crtp_base<dense_union_array>::erase(const_iterator, const_iterator) -> iterator;
    template SPARROW_API auto union_array_crtp_base<dense_union_array>::erase_values(size_type, size_type) -> iterator;
    template SPARROW_API auto union_array_crtp_base<dense_union_array>::rebuild_values(
        std::vector<std::pair<size_type, array_traits::value_type>>,
        size_type
    ) -> iterator;
    template SPARROW_API void union_array_crtp_base<dense_union_array>::push_back(const_reference);
    template SPARROW_API void union_array_crtp_base<dense_union_array>::push_back(const array_traits::value_type&);
    template SPARROW_API void union_array_crtp_base<dense_union_array>::resize(size_type);
    template SPARROW_API void union_array_crtp_base<dense_union_array>::resize(size_type, const_reference);
    template SPARROW_API void union_array_crtp_base<dense_union_array>::resize(size_type, const array_traits::value_type&);

    template SPARROW_API auto union_array_crtp_base<sparse_union_array>::insert(
        const_iterator,
        const_reference,
        size_type
    ) -> iterator;
    template SPARROW_API auto union_array_crtp_base<sparse_union_array>::insert(
        const_iterator,
        const array_traits::value_type&,
        size_type
    ) -> iterator;
    template SPARROW_API auto union_array_crtp_base<sparse_union_array>::erase(const_iterator) -> iterator;
    template SPARROW_API auto union_array_crtp_base<sparse_union_array>::erase(const_iterator, const_iterator) -> iterator;
    template SPARROW_API auto union_array_crtp_base<sparse_union_array>::erase_values(size_type, size_type) -> iterator;
    template SPARROW_API auto union_array_crtp_base<sparse_union_array>::rebuild_values(
        std::vector<std::pair<size_type, array_traits::value_type>>,
        size_type
    ) -> iterator;
    template SPARROW_API void union_array_crtp_base<sparse_union_array>::push_back(const_reference);
    template SPARROW_API void union_array_crtp_base<sparse_union_array>::push_back(const array_traits::value_type&);
    template SPARROW_API void union_array_crtp_base<sparse_union_array>::resize(size_type);
    template SPARROW_API void union_array_crtp_base<sparse_union_array>::resize(size_type, const_reference);
    template SPARROW_API void union_array_crtp_base<sparse_union_array>::resize(size_type, const array_traits::value_type&);
}
