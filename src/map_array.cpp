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

#include "sparrow/map_array.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>
#include <vector>

#include "sparrow/array.hpp"
#include "sparrow/debug/copy_tracker.hpp"
#include "sparrow/layout/array_helper.hpp"

namespace sparrow
{
    namespace copy_tracker
    {
        template <>
        SPARROW_API std::string key<map_array>()
        {
            return "map_array";
        }
    }

    map_array::map_array(arrow_proxy proxy)
        : base_type(std::move(proxy))
        , p_entries_array(make_entries_array())
        , m_keys_sorted(get_keys_sorted())
    {
        visit(
            []<class T>(const T&)
            {
                using value_type = typename T::value_type;
                if constexpr (mpl::is_type_instance_of<value_type, nullable_variant>::value)
                {
                    throw std::runtime_error("Array of variants cannot be used as array of keys in a map_array");
                }
            },
            *(raw_keys_array())
        );
    }

    map_array::map_array(const self_type& rhs)
        : base_type(rhs)
        , p_entries_array(make_entries_array())
        , m_keys_sorted(rhs.m_keys_sorted)
    {
        copy_tracker::increase(copy_tracker::key<map_array>());
    }

    map_array& map_array::operator=(const self_type& rhs)
    {
        copy_tracker::increase(copy_tracker::key<map_array>());
        if (this != &rhs)
        {
            base_type::operator=(rhs);
            p_entries_array = make_entries_array();
            m_keys_sorted = rhs.m_keys_sorted;
        }
        return *this;
    }

    const array_wrapper* map_array::raw_keys_array() const
    {
        return unwrap_array<struct_array>(*p_entries_array).raw_child(std::size_t(0));
    }

    array_wrapper* map_array::raw_keys_array()
    {
        return unwrap_array<struct_array>(*p_entries_array).raw_child(std::size_t(0));
    }

    const array_wrapper* map_array::raw_items_array() const
    {
        return unwrap_array<struct_array>(*p_entries_array).raw_child(std::size_t(1));
    }

    array_wrapper* map_array::raw_items_array()
    {
        return unwrap_array<struct_array>(*p_entries_array).raw_child(std::size_t(1));
    }

    auto map_array::value_begin() -> value_iterator
    {
        return value_iterator(value_iterator::functor_type(this), 0);
    }

    auto map_array::value_end() -> value_iterator
    {
        return value_iterator(value_iterator::functor_type(this), this->size());
    }

    auto map_array::value_cbegin() const -> const_value_iterator
    {
        return const_value_iterator(const_value_iterator::functor_type(this), 0);
    }

    auto map_array::value_cend() const -> const_value_iterator
    {
        return const_value_iterator(const_value_iterator::functor_type(this), this->size());
    }

    auto map_array::value(size_type i) -> inner_reference
    {
        return static_cast<const map_array*>(this)->value(i);
    }

    auto map_array::value(size_type i) const -> inner_const_reference
    {
        const auto offsets = make_list_offsets();
        return {raw_keys_array(), raw_items_array(), offsets[i], offsets[i + 1], m_keys_sorted};
    }

    auto map_array::make_list_offsets() const -> offset_span_type
    {
        const auto& proxy = this->get_arrow_proxy();
        const auto& offset_buffer = proxy.buffers()[OFFSET_BUFFER_INDEX];
        const auto offset_count = offset_buffer.size() / sizeof(std::int32_t);
        return offset_span_type(offset_buffer.template data<std::int32_t>(), offset_count)
            .subspan(static_cast<std::size_t>(proxy.offset()));
    }

    cloning_ptr<array_wrapper> map_array::make_entries_array() const
    {
        return array_factory(this->get_arrow_proxy().children()[0].view());
    }

    bool map_array::get_keys_sorted() const
    {
        return this->get_arrow_proxy().flags().contains(ArrowFlag::MAP_KEYS_SORTED);
    }

    bool map_array::check_keys_sorted(const array& flat_keys, const offset_buffer_type& offsets)
    {
        bool sorted = true;
        for (std::size_t i = 0; i + 1 < offsets.size(); ++i)
        {
            const std::size_t index_begin = offsets[i];
            const std::size_t index_end = offsets[i + 1];
            sorted = flat_keys.visit(
                [index_begin, index_end]<class T>(const T& ar) -> bool
                {
                    bool isorted = true;
                    if constexpr (std::three_way_comparable<typename T::const_reference>)
                    {
                        isorted = true;
                        for (std::size_t j = index_begin; j + 1 < index_end; ++j)
                        {
                            isorted = (ar[j] < ar[j + 1]);
                            if (!isorted)
                            {
                                break;
                            }
                        }
                    }
                    return isorted;
                }
            );
            if (!sorted)
            {
                break;
            }
        }
        return sorted;
    }

    void map_array::replace_contents(
        std::vector<array_traits::value_type>&& flat_keys,
        std::vector<array_traits::value_type>&& flat_items,
        offset_buffer_type&& list_offsets
    )
    {
        array keys_array = array_empty_like(make_array_view(*raw_keys_array()));
        array items_array = array_empty_like(make_array_view(*raw_items_array()));
        append_values(keys_array, std::move(flat_keys));
        append_values(items_array, std::move(flat_items));

        if (!check_keys_sorted(keys_array, list_offsets))
        {
            throw std::invalid_argument("Map keys must be strictly increasing");
        }

        auto& entries = unwrap_array<struct_array>(*p_entries_array);
        auto& entries_proxy = detail::array_access::get_arrow_proxy(entries);
        entries_proxy.set_length(flat_keys.size());
        entries.set_child(std::move(keys_array), 0);
        entries.set_child(std::move(items_array), 1);

        get_arrow_proxy().set_buffer(OFFSET_BUFFER_INDEX, std::move(list_offsets).extract_storage());

        auto flags = get_arrow_proxy().flags();
        flags.insert(ArrowFlag::MAP_KEYS_SORTED);
        get_arrow_proxy().set_flags(flags);
        m_keys_sorted = true;
    }

    namespace
    {
        using dynamic_value = array::value_type;

        struct mutation_snapshot
        {
            std::vector<dynamic_value> keys;
            std::vector<dynamic_value> items;
        };

        mutation_snapshot snapshot_for_mutation(
            const map_array& map,
            bool keys_sorted,
            const char* operation
        )
        {
            const auto& proxy = detail::array_access::get_arrow_proxy(map);
            if (proxy.offset() != 0)
            {
                throw std::logic_error(operation);
            }
            if (!keys_sorted)
            {
                throw std::invalid_argument("Cannot mutate a map_array with unsorted keys");
            }

            return {
                .keys=snapshot_array(make_array_view(*map.raw_keys_array())),
                .items=snapshot_array(make_array_view(*map.raw_items_array()))
            };
        }

        template <bool MOVE_SOURCE>
        void append_range(
            std::vector<dynamic_value>& destination,
            std::vector<dynamic_value>& source,
            std::size_t begin,
            std::size_t end
        )
        {
            auto first = source.begin() + static_cast<std::ptrdiff_t>(begin);
            auto last = source.begin() + static_cast<std::ptrdiff_t>(end);
            if constexpr (MOVE_SOURCE)
            {
                destination.insert(
                    destination.end(),
                    std::make_move_iterator(first),
                    std::make_move_iterator(last)
                );
            }
            else
            {
                destination.insert(destination.end(), first, last);
            }
        }

        /**
         * @brief Appends the entries [begin, end) of the source lists and records the new offset.
         *
         * With MOVE_SOURCE, the entries are moved out of the source lists instead of being
         * copied. The caller guarantees each source entry is appended at most once.
         */
        template <bool MOVE_SOURCE>
        void append_entry(
            std::vector<dynamic_value>& destination_keys,
            std::vector<dynamic_value>& destination_items,
            map_array::offset_buffer_type& destination_offsets,
            std::vector<dynamic_value>& source_keys,
            std::vector<dynamic_value>& source_items,
            std::size_t begin,
            std::size_t end
        )
        {
            append_range<MOVE_SOURCE>(destination_keys, source_keys, begin, end);
            append_range<MOVE_SOURCE>(destination_items, source_items, begin, end);
            if (!std::in_range<std::int32_t>(destination_keys.size()))
            {
                throw std::overflow_error("Map entries exceed the int32 offset range");
            }
            destination_offsets.push_back(static_cast<std::int32_t>(destination_keys.size()));
        }

        struct rebuilt_entries
        {
            rebuilt_entries()
                : offsets(0)
            {
            }

            std::vector<dynamic_value> keys;
            std::vector<dynamic_value> items;
            map_array::offset_buffer_type offsets;
        };

        /**
         * @brief Rebuilds the flat key/item lists and offsets from an explicit row plan.
         *
         * Old rows are stored as a separate index array. Inserted rows are represented
         * by their insertion position and count instead of per-row records. Each old
         * row copies the entries [offsets[row], offsets[row + 1]) of the old flat lists;
         * each inserted row copies the entries [0, inserted_entry_size) of the inserted lists.
         *
         * @return The rebuilt flat lists and a fresh offset buffer (starting at 0).
         */
        rebuilt_entries rebuild_flat_entries(
            std::vector<dynamic_value> old_keys,
            std::vector<dynamic_value> old_items,
            std::span<const std::int32_t> old_offsets,
            std::span<const std::size_t> old_rows,
            std::size_t inserted_at,
            std::size_t inserted_count,
            std::vector<dynamic_value> inserted_keys,
            std::vector<dynamic_value> inserted_items,
            std::size_t inserted_entry_size
        )
        {
            rebuilt_entries out;
            out.keys.reserve(old_keys.size() + inserted_count * inserted_entry_size);
            out.items.reserve(old_items.size() + inserted_count * inserted_entry_size);
            out.offsets.reserve(old_rows.size() + inserted_count + 1);
            out.offsets.push_back(0);

            const auto append_old_row = [&](std::size_t old_row)
            {
                const auto begin = static_cast<std::size_t>(old_offsets[old_row]);
                const auto end = static_cast<std::size_t>(old_offsets[old_row + 1]);
                append_entry<true>(out.keys, out.items, out.offsets, old_keys, old_items, begin, end);
            };
            const auto append_inserted_row = [&]
            {
                append_entry<false>(
                    out.keys,
                    out.items,
                    out.offsets,
                    inserted_keys,
                    inserted_items,
                    0,
                    inserted_entry_size
                );
            };

            const auto old_prefix_count = std::min(inserted_at, old_rows.size());
            for (std::size_t i = 0; i < old_prefix_count; ++i)
            {
                append_old_row(old_rows[i]);
            }
            for (std::size_t i = 0; i < inserted_count; ++i)
            {
                append_inserted_row();
            }
            for (std::size_t i = old_prefix_count; i < old_rows.size(); ++i)
            {
                append_old_row(old_rows[i]);
            }
            return out;
        }
    }

    void map_array::resize_values(size_type new_length, const map_value& value)
    {
        const size_type current_size = this->size();
        if (new_length < current_size)
        {
            erase_values(
                std::next(value_cbegin(), static_cast<std::ptrdiff_t>(new_length)),
                current_size - new_length
            );
        }
        else if (new_length > current_size)
        {
            insert_value(value_cend(), value, new_length - current_size);
        }
    }

    map_array::value_iterator
    map_array::insert_value(const_value_iterator pos, const map_value& value, size_type count)
    {
        const auto index = static_cast<size_type>(std::distance(value_cbegin(), pos));
        if (count == 0)
        {
            return std::next(value_begin(), static_cast<std::ptrdiff_t>(index));
        }
        auto [old_keys, old_items] = snapshot_for_mutation(
            *this,
            m_keys_sorted,
            "map_array::insert_value does not support sliced arrays"
        );

        std::vector<dynamic_value> inserted_keys;
        inserted_keys.reserve(value.size());
        std::vector<dynamic_value> inserted_items;
        inserted_items.reserve(value.size());
        for (const auto& entry : value)
        {
            inserted_keys.push_back(array_materialize_element(entry.first));
            inserted_items.push_back(array_materialize_element(entry.second));
        }

        const size_type old_size = size();
        const auto old_offsets = make_list_offsets();
        std::vector<std::size_t> old_rows;
        old_rows.reserve(old_size);
        for (size_type row = 0; row < old_size; ++row)
        {
            old_rows.push_back(row);
        }

        auto rebuilt = rebuild_flat_entries(
            std::move(old_keys),
            std::move(old_items),
            old_offsets,
            old_rows,
            index,
            count,
            std::move(inserted_keys),
            std::move(inserted_items),
            value.size()
        );
        replace_contents(
            std::move(rebuilt.keys),
            std::move(rebuilt.items),
            std::move(rebuilt.offsets)
        );
        return std::next(value_begin(), static_cast<std::ptrdiff_t>(index));
    }

    map_array::value_iterator map_array::erase_values(const_value_iterator pos, size_type count)
    {
        const auto index = static_cast<size_type>(std::distance(value_cbegin(), pos));
        if (count == 0)
        {
            return std::next(value_begin(), static_cast<std::ptrdiff_t>(index));
        }
        auto [old_keys, old_items] = snapshot_for_mutation(*this, m_keys_sorted, "map_array::erase_values");
        const size_type old_size = size();
        const auto old_offsets = make_list_offsets();

        std::vector<std::size_t> old_rows;
        old_rows.reserve(old_size - count);
        for (size_type row = 0; row < old_size; ++row)
        {
            if (row < index || row >= index + count)
            {
                old_rows.push_back(row);
            }
        }

        auto rebuilt = rebuild_flat_entries(
            std::move(old_keys),
            std::move(old_items),
            old_offsets,
            old_rows,
            old_rows.size(),
            0,
            {},
            {},
            0
        );
        replace_contents(
            std::move(rebuilt.keys),
            std::move(rebuilt.items),
            std::move(rebuilt.offsets)
        );
        return std::next(value_begin(), static_cast<std::ptrdiff_t>(index));
    }
}
