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
#include <string>
#include <vector>

#include "sparrow/array.hpp"
#include "sparrow/debug/copy_tracker.hpp"
#include "sparrow/null_array.hpp"

namespace sparrow
{
    namespace
    {
        using dynamic_value = array_traits::value_type;
        using rebuild_value = detail::union_rebuild_value;
        using rebuild_source = detail::union_rebuild_source;

        constexpr std::size_t no_child = std::numeric_limits<std::size_t>::max();

        array make_empty_child(const array_wrapper& child)
        {
            const array view{child.get_arrow_proxy().view()};
            return array_empty_like(view);
        }

        /**
         * @brief Builds a null dynamic value holding the type of the given inner value.
         * @tparam T The type of the inner value (nullable alternatives are unwrapped by
         *           the caller, e.g. via get()).
         * @param value An inner value only used for its type.
         * @return A dynamic_value of the same type, in null state.
         */
        template <class T>
        dynamic_value make_null_dynamic_value(const T& value)
        {
            using stored_type = std::remove_cvref_t<T>;
            return dynamic_value(nullable<stored_type>(stored_type(value), false));
        }

        /**
         * @brief Creates a null-like dynamic value for the given dynamic value.
         * @param value The dynamic value for which to create a null-like value.
         * @return A dynamic_value representing the null-like value.
         */
        dynamic_value make_null_like(const dynamic_value& value)
        {
            return std::visit(
                [](const auto& typed_value) -> dynamic_value
                {
                    return make_null_dynamic_value(typed_value.get());
                },
#if SPARROW_GCC_11_2_WORKAROUND
                static_cast<const dynamic_value::base_type&>(value)
#else
                value
#endif
            );
        }

        /**
        * @brief Creates a null value for the given array wrapper.
        * @param child The array wrapper for which to create a null value.
        * @return A dynamic_value representing the null value.
        */
        dynamic_value make_null_value(const array_wrapper& child)
        {
            return make_null_like(array_default_value(child));
        }

        struct union_rebuild_payload
        {
            std::vector<std::vector<rebuild_value>> child_values;
            std::vector<std::uint8_t> type_ids;
            std::vector<std::uint32_t> offsets;
        };

        /**
         * @brief Creates a rebuild payload for a dense union array.
         * @param values The values to be rebuilt.
         * @param child_type_ids The type IDs of the child arrays.
         * @return A union_rebuild_payload containing the rebuilt data.
         */
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

            for (auto& value : values)
            {
                const auto child_index = value.first;
                payload.type_ids.push_back(child_type_ids[child_index]);
                SPARROW_ASSERT_TRUE(
                    payload.child_values[child_index].size()
                    <= static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())
                );
                payload.offsets.push_back(static_cast<std::uint32_t>(payload.child_values[child_index].size()));
                payload.child_values[child_index].push_back(std::move(value));
            }
            return payload;
        }

        /**
         * @brief Creates a rebuild payload for a sparse union array.
         * @tparam CHILDREN The type of the old children array wrappers.
         * @param values The values to be rebuilt.
         * @param child_type_ids The type IDs of the child arrays.
         * @param old_children The old children array wrappers.
         * @return A union_rebuild_payload containing the rebuilt data.
         */
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

            std::vector<std::optional<rebuild_value>> null_values(child_type_ids.size());
            for (std::size_t child_index = 0; child_index < old_children.size(); ++child_index)
            {
                const auto& old_child = *old_children[child_index];
                const auto child_size = array_size(old_child);
                if (child_size == 0)
                {
                    continue;
                }

                std::size_t null_index = 0;
                while (null_index < child_size && array_has_value(old_child, null_index))
                {
                    ++null_index;
                }
                null_values[child_index] = rebuild_value{
                    child_index,
                    rebuild_source{
                        null_index == child_size ? std::size_t{0} : null_index,
                        null_index == child_size
                    }
                };
            }
            for (auto& value : values)
            {
                const auto child_index = value.first;
                payload.type_ids.push_back(child_type_ids[child_index]);
                if (!null_values[child_index].has_value())
                {
                    if (const auto* inserted_value = std::get_if<dynamic_value>(&value.second))
                    {
                        null_values[child_index] = rebuild_value{
                            child_index,
                            make_null_like(*inserted_value)
                        };
                    }
                }
            }

            for (std::size_t child_index = 0; child_index < child_type_ids.size(); ++child_index)
            {
                if (!null_values[child_index].has_value() && child_index < old_children.size())
                {
                    null_values[child_index] = rebuild_value{
                        child_index,
                        make_null_value(*old_children[child_index])
                    };
                }
                SPARROW_ASSERT_TRUE(null_values[child_index].has_value() || values.empty());

                payload.child_values[child_index].reserve(values.size());
                for (std::size_t row = 0; row < values.size(); ++row)
                {
                    if (values[row].first == child_index)
                    {
                        payload.child_values[child_index].push_back(std::move(values[row]));
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
        return static_cast<std::size_t>(p_offsets[i + m_proxy.offset()]);
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
                                           rebuild_source{this->derived_cast().element_offset(i), false}
                                       };
                                   }
                               );
        std::ranges::copy(retained_values, std::back_inserter(values));
        return rebuild_values(std::move(values), first);
    }

    template <typename CHILDREN>
    void append_rebuild_value(array& destination, rebuild_value& value, const CHILDREN& old_children)
    {
        std::visit(
            [&destination, &value, &old_children](auto& source)
            {
                using source_type = std::remove_cvref_t<decltype(source)>;
                if constexpr (std::same_as<source_type, rebuild_source>)
                {
                    const auto& old_child = *old_children[value.first];
                    const array child_view{old_child.get_arrow_proxy().view()};
                    array source_array = child_view.slice(source.index, source.index + 1);
                    if (source.force_null)
                    {
                        auto& source_proxy = detail::array_access::get_arrow_proxy(source_array);
                        auto& bitmap = source_proxy.bitmap();
                        SPARROW_ASSERT_TRUE(bitmap.has_value());
                        *bitmap->begin() = false;
                        source_proxy.set_null_count(1);
                    }
                    destination.insert(destination.cend(), source_array.cbegin(), source_array.cend());
                }
                else
                {
                    array source_array = array_make_from_element(std::move(source));
                    destination.insert(destination.cend(), source_array.cbegin(), source_array.cend());
                }
            },
            value.second
        );
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::rebuild_values(
        std::vector<rebuild_value> values,
        size_type return_index
    ) -> iterator
    {
        const auto old_child_count = m_children.size();
        SPARROW_ASSERT_TRUE(m_child_type_ids.size() == old_child_count);
        std::vector<array> child_templates(old_child_count);
        std::vector<std::uint8_t> child_type_ids = m_child_type_ids;
        std::array<bool, TYPE_ID_MAP_SIZE> used_type_ids{};
        struct child_schema
        {
            child_schema(data_type type_, std::string format_)
                : type(type_)
                , format(std::move(format_))
            {
            }

            data_type type;
            std::string format;
        };

        std::vector<child_schema> child_schemas;
        child_schemas.reserve(old_child_count);
        for (std::size_t child_index = 0; child_index < old_child_count; ++child_index)
        {
            used_type_ids[child_type_ids[child_index]] = true;
            child_schemas.emplace_back(
                m_children[child_index]->data_type(),
                std::string(m_children[child_index]->get_arrow_proxy().format())
            );
        }

        std::vector<std::pair<child_schema, size_type>> value_schema_to_child;
        auto resolve_child_index = [&](const dynamic_value& value) -> size_type
        {
            auto child = array_make_from_element(value);
            child_schema schema{
                child.data_type(),
                std::string(detail::array_access::get_arrow_proxy(child).format())
            };
            const auto cached = std::find_if(
                value_schema_to_child.begin(),
                value_schema_to_child.end(),
                [&schema](const auto& entry)
                {
                    return entry.first.type == schema.type && entry.first.format == schema.format;
                }
            );
            if (cached != value_schema_to_child.end())
            {
                return cached->second;
            }

            const auto existing = std::find_if(
                child_schemas.begin(),
                child_schemas.end(),
                [&schema](const child_schema& existing_schema)
                {
                    return existing_schema.type == schema.type && existing_schema.format == schema.format;
                }
            );
            if (existing != child_schemas.end())
            {
                const auto child_index = static_cast<size_type>(existing - child_schemas.begin());
                value_schema_to_child.emplace_back(std::move(schema), child_index);
                return child_index;
            }

            SPARROW_ASSERT_TRUE(child_schemas.size() < 256);
            std::size_t type_id = 0;
            while (type_id < TYPE_ID_MAP_SIZE && used_type_ids[type_id])
            {
                ++type_id;
            }
            SPARROW_ASSERT_TRUE(type_id < TYPE_ID_MAP_SIZE);
            used_type_ids[type_id] = true;
            const auto child_index = child_schemas.size();
            child_schemas.push_back(std::move(schema));
            child_type_ids.push_back(static_cast<std::uint8_t>(type_id));
            child_templates.push_back(std::move(child));
            value_schema_to_child.emplace_back(child_schemas.back(), child_index);
            return child_index;
        };

        for (auto& value : values)
        {
            if (value.first == no_child)
            {
                const auto* inserted_value = std::get_if<dynamic_value>(&value.second);
                SPARROW_ASSERT_TRUE(inserted_value != nullptr);
                value.first = resolve_child_index(*inserted_value);
            }
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
        new_children.reserve(child_schemas.size());
        for (std::size_t child_index = 0; child_index < child_schemas.size(); ++child_index)
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
                for (auto& value : values_for_child)
                {
                    append_rebuild_value(child, value, m_children);
                }
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
            this->derived_cast().p_offsets = reinterpret_cast<std::int32_t*>(m_proxy.buffers()[1].data());
        }

        return iterator(functor_type{&this->derived_cast()}, return_index);
    }

#define SPARROW_INSTANTIATE_UNION_CRTP_BASE(TYPE)                                                        \
    template SPARROW_API auto union_array_crtp_base<TYPE>::insert(                                        \
        const_iterator,                                                                                   \
        const_reference,                                                                                  \
        size_type                                                                                         \
    ) -> iterator;                                                                                        \
    template SPARROW_API auto union_array_crtp_base<TYPE>::insert(                                        \
        const_iterator,                                                                                   \
        const array_traits::value_type&,                                                                  \
        size_type                                                                                         \
    ) -> iterator;                                                                                        \
    template SPARROW_API auto union_array_crtp_base<TYPE>::erase(const_iterator) -> iterator;             \
    template SPARROW_API auto union_array_crtp_base<TYPE>::erase(const_iterator, const_iterator)          \
        -> iterator;                                                                                      \
    template SPARROW_API auto union_array_crtp_base<TYPE>::erase_values(size_type, size_type)             \
        -> iterator;                                                                                      \
    template SPARROW_API auto union_array_crtp_base<TYPE>::rebuild_values(                                \
        std::vector<detail::union_rebuild_value>,                                                        \
        size_type                                                                                         \
    ) -> iterator;                                                                                        \
    template SPARROW_API void union_array_crtp_base<TYPE>::push_back(const_reference);                    \
    template SPARROW_API void union_array_crtp_base<TYPE>::push_back(const array_traits::value_type&);    \
    template SPARROW_API void union_array_crtp_base<TYPE>::resize(size_type);                             \
    template SPARROW_API void union_array_crtp_base<TYPE>::resize(size_type, const_reference);            \
    template SPARROW_API void union_array_crtp_base<TYPE>::resize(                                        \
        size_type,                                                                                        \
        const array_traits::value_type&                                                                   \
    );                                                                                                    \
    template SPARROW_API union_array_crtp_base<TYPE>::union_array_crtp_base(arrow_proxy);                 \
    template SPARROW_API auto union_array_crtp_base<TYPE>::child_type_ids_from_format(                    \
        std::string_view                                                                                  \
    ) -> std::vector<std::uint8_t>;                                                                       \
    template SPARROW_API auto union_array_crtp_base<TYPE>::make_type_id_map(                              \
        std::span<const std::uint8_t>                                                                     \
    ) -> type_id_map;                                                                                     \
    template SPARROW_API auto union_array_crtp_base<TYPE>::operator=(                                     \
        const union_array_crtp_base<TYPE>&                                                                \
    ) -> union_array_crtp_base<TYPE>&;                                                                    \
    template SPARROW_API auto union_array_crtp_base<TYPE>::make_children(arrow_proxy&) -> children_type;  \
    template SPARROW_API union_array_crtp_base<TYPE>::union_array_crtp_base(                              \
        const union_array_crtp_base<TYPE>&                                                                \
    );                                                                                                    \
    template SPARROW_API auto union_array_crtp_base<TYPE>::name() const                                   \
        -> std::optional<std::string_view>;                                                               \
    template SPARROW_API auto union_array_crtp_base<TYPE>::metadata() const                               \
        -> std::optional<key_value_view>;                                                                 \
    template SPARROW_API auto union_array_crtp_base<TYPE>::operator[](size_type) const -> value_type;      \
    template SPARROW_API auto union_array_crtp_base<TYPE>::operator[](size_type) -> value_type;           \
    template SPARROW_API auto union_array_crtp_base<TYPE>::front() const -> value_type;                   \
    template SPARROW_API auto union_array_crtp_base<TYPE>::empty() const -> bool;                         \
    template SPARROW_API auto union_array_crtp_base<TYPE>::size() const -> size_type;                     \
    template SPARROW_API auto union_array_crtp_base<TYPE>::begin() -> iterator;                           \
    template SPARROW_API auto union_array_crtp_base<TYPE>::end() -> iterator;                             \
    template SPARROW_API auto union_array_crtp_base<TYPE>::begin() const -> const_iterator;               \
    template SPARROW_API auto union_array_crtp_base<TYPE>::end() const -> const_iterator;                 \
    template SPARROW_API auto union_array_crtp_base<TYPE>::cbegin() const -> const_iterator;              \
    template SPARROW_API auto union_array_crtp_base<TYPE>::cend() const -> const_iterator;                \
    template SPARROW_API auto union_array_crtp_base<TYPE>::rbegin() const -> const_reverse_iterator;      \
    template SPARROW_API auto union_array_crtp_base<TYPE>::rend() const -> const_reverse_iterator;        \
    template SPARROW_API auto union_array_crtp_base<TYPE>::crbegin() const -> const_reverse_iterator;     \
    template SPARROW_API auto union_array_crtp_base<TYPE>::crend() const -> const_reverse_iterator;       \
    template SPARROW_API auto union_array_crtp_base<TYPE>::back() const -> value_type;                    \
    template SPARROW_API auto union_array_crtp_base<TYPE>::get_arrow_proxy() -> arrow_proxy&;             \
    template SPARROW_API auto union_array_crtp_base<TYPE>::get_arrow_proxy() const                        \
        -> const arrow_proxy&;

    SPARROW_INSTANTIATE_UNION_CRTP_BASE(dense_union_array)
    SPARROW_INSTANTIATE_UNION_CRTP_BASE(sparse_union_array)

#undef SPARROW_INSTANTIATE_UNION_CRTP_BASE
}
