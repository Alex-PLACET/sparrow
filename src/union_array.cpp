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

    auto dense_union_array::rebuild_in_place(
        std::vector<detail::union_rebuild_value> values,
        size_type return_index
    ) -> iterator
    {
        return union_array_crtp_base<dense_union_array>::rebuild_values(std::move(values), return_index);
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

    auto sparse_union_array::rebuild_in_place(
        std::vector<detail::union_rebuild_value> values,
        size_type return_index
    ) -> iterator
    {
        return union_array_crtp_base<sparse_union_array>::rebuild_values(std::move(values), return_index);
    }

    /**
     * @brief Slices the row a filler rebuild value refers to, marked as null.
     *
     * Bitmap-less children (e.g. a nested union) cannot hold a null in a single row: the row
     * keeps its value, sparse-union filler rows being unspecified by the Arrow format.
     */
    array make_filler_row(const array& old_child_view, std::size_t row)
    {
        array filler = old_child_view.slice(row, row + 1);
        auto& proxy = detail::array_access::get_arrow_proxy(filler);
        if (auto& bitmap = proxy.bitmap(); bitmap.has_value())
        {
            *bitmap->begin() = false;
            proxy.set_null_count(1);
        }
        return filler;
    }

    /**
     * @brief Appends @p count consecutive rows of an old child, in a single insertion.
     *
     * The slice is borrowed: the old child outlives the insertion (the children wrappers are
     * only rebuilt once the new proxy is in place), and a deep copy per insertion is what made
     * the rebuild quadratic in the child size.
     */
    void append_old_child_values(array& destination, const array& old_child_view, std::size_t first_row, std::size_t count)
    {
        const array rows = old_child_view.slice_view(first_row, first_row + count);
        destination.insert(destination.cend(), rows.cbegin(), rows.cend());
    }

    /**
     * @brief Appends the rebuild values of one child to @p destination.
     *
     * Values reading consecutive rows of the same old child are appended with a single range
     * insertion, and filler rows with a single repeated insertion of one shared row. Appending
     * value by value slices the old child once per value and rebuilds nested unions once per
     * value, which is quadratic.
     */
    template <typename CHILDREN>
    void append_rebuild_values(array& destination, std::vector<rebuild_value>& values, const CHILDREN& old_children)
    {
        // View over the old child the values are read from, built once per child.
        std::optional<array> old_child_view;
        // Every filler row of a child refers to the same old child row: slice it once.
        std::optional<array> filler_row;

        for (std::size_t index = 0; index < values.size();)
        {
            const auto* source = std::get_if<rebuild_source>(&values[index].second);
            if (source == nullptr)
            {
                array inserted = array_make_from_element(std::move(std::get<dynamic_value>(values[index].second)));
                destination.insert(destination.cend(), inserted.cbegin(), inserted.cend());
                ++index;
                continue;
            }

            std::size_t count = 1;
            while (index + count < values.size())
            {
                const auto* next = std::get_if<rebuild_source>(&values[index + count].second);
                if (next == nullptr || values[index + count].first != values[index].first
                    || next->force_null != source->force_null
                    || (source->force_null ? next->index != source->index
                                           : next->index != source->index + count))
                {
                    break;
                }
                ++count;
            }

            if (!old_child_view.has_value())
            {
                old_child_view = array{old_children[values[index].first]->get_arrow_proxy().view()};
            }

            if (source->force_null)
            {
                if (!filler_row.has_value())
                {
                    filler_row = make_filler_row(*old_child_view, source->index);
                }
                destination.insert(destination.cend(), filler_row->cbegin(), filler_row->cend(), count);
            }
            else
            {
                append_old_child_values(destination, *old_child_view, source->index, count);
            }
            index += count;
        }
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::rebuild_values(
        std::vector<rebuild_value> values,
        size_type return_index
    ) -> iterator
    {
        const auto old_child_count = m_children.size();
        SPARROW_ASSERT_TRUE(m_child_type_ids.size() == old_child_count);
        std::vector<std::uint8_t> child_type_ids = m_child_type_ids;
        std::array<bool, TYPE_ID_MAP_SIZE> used_type_ids{};

        struct child_schema
        {
            data_type type;
            std::string format;

            [[nodiscard]] bool operator==(const child_schema& other) const = default;
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

        // Prototypes of the children created for the inserted values, indexed by
        // child_index - old_child_count.
        std::vector<array> new_child_templates;

        auto resolve_child_index = [&](const dynamic_value& value) -> size_type
        {
            auto child = array_make_from_element(value);
            const child_schema schema{
                child.data_type(),
                std::string(detail::array_access::get_arrow_proxy(child).format())
            };
            const auto existing = std::ranges::find(child_schemas, schema);
            if (existing != child_schemas.end())
            {
                return static_cast<size_type>(existing - child_schemas.begin());
            }

            SPARROW_ASSERT_TRUE(child_schemas.size() < TYPE_ID_MAP_SIZE);
            std::size_t type_id = 0;
            while (type_id < TYPE_ID_MAP_SIZE && used_type_ids[type_id])
            {
                ++type_id;
            }
            SPARROW_ASSERT_TRUE(type_id < TYPE_ID_MAP_SIZE);
            used_type_ids[type_id] = true;

            const auto child_index = child_schemas.size();
            child_schemas.push_back(schema);
            child_type_ids.push_back(static_cast<std::uint8_t>(type_id));
            new_child_templates.push_back(std::move(child));
            return child_index;
        };

        for (auto& value : values)
        {
            if (value.first == detail::union_no_child)
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
                              : std::move(new_child_templates[child_index - old_child_count]);
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
                append_rebuild_values(child, values_for_child, m_children);
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
}
