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
        , p_list_offsets(make_list_offsets())
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
        , p_list_offsets(make_list_offsets())
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
            p_list_offsets = make_list_offsets();
            p_entries_array = make_entries_array();
            m_keys_sorted = rhs.m_keys_sorted;
        }
        return *this;
    }

    const array_wrapper* map_array::raw_keys_array() const
    {
        return unwrap_array<struct_array>(*p_entries_array).raw_child(std::size_t(0));
        ;
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
        return map_value(raw_keys_array(), raw_items_array(), p_list_offsets[i], p_list_offsets[i + 1], m_keys_sorted);
    }

    auto map_array::make_list_offsets() const -> offset_type*
    {
        return reinterpret_cast<offset_type*>(
            this->get_arrow_proxy().buffers()[OFFSET_BUFFER_INDEX].data() + this->get_arrow_proxy().offset()
        );
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
            std::size_t index_begin = offsets[i];
            std::size_t index_end = offsets[i + 1];
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
        append_values(keys_array, flat_keys);
        append_values(items_array, flat_items);

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
        p_list_offsets = make_list_offsets();

        auto flags = get_arrow_proxy().flags();
        flags.insert(ArrowFlag::MAP_KEYS_SORTED);
        get_arrow_proxy().set_flags(flags);
        m_keys_sorted = true;
    }

    namespace
    {
        using dynamic_value = array::value_type;

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
            auto keys_first = source_keys.begin() + static_cast<std::ptrdiff_t>(begin);
            auto keys_last = source_keys.begin() + static_cast<std::ptrdiff_t>(end);
            auto items_first = source_items.begin() + static_cast<std::ptrdiff_t>(begin);
            auto items_last = source_items.begin() + static_cast<std::ptrdiff_t>(end);
            if constexpr (MOVE_SOURCE)
            {
                destination_keys.insert(
                    destination_keys.end(),
                    std::make_move_iterator(keys_first),
                    std::make_move_iterator(keys_last)
                );
                destination_items.insert(
                    destination_items.end(),
                    std::make_move_iterator(items_first),
                    std::make_move_iterator(items_last)
                );
            }
            else
            {
                destination_keys.insert(destination_keys.end(), keys_first, keys_last);
                destination_items.insert(destination_items.end(), items_first, items_last);
            }
            SPARROW_ASSERT_TRUE(std::in_range<std::int32_t>(destination_keys.size()));
            destination_offsets.push_back(static_cast<std::int32_t>(destination_keys.size()));
        }

        /**
         * One row of the rebuilt map: either an old row (is_inserted == false,
         * old_row = its index) or a copy of the inserted entry (is_inserted == true).
         */
        struct row_source
        {
            row_source(bool inserted, std::size_t old_row_index)
                : is_inserted(inserted)
                , old_row(old_row_index)
            {
            }

            bool is_inserted;
            std::size_t old_row;
        };

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
         * Rows are appended in plan order; each old row copies the entries
         * [offsets[row], offsets[row + 1]) of the old flat lists, each inserted row
         * copies the entries [0, inserted_entry_size) of the inserted lists.
         *
         * @return The rebuilt flat lists and a fresh offset buffer (starting at 0).
         */
        rebuilt_entries rebuild_flat_entries(
            std::vector<dynamic_value>&& old_keys,
            std::vector<dynamic_value>&& old_items,
            map_array::offset_type* old_offsets,
            std::span<const row_source> rows,
            std::vector<dynamic_value>&& inserted_keys,
            std::vector<dynamic_value>&& inserted_items,
            std::size_t inserted_entry_size
        )
        {
            rebuilt_entries out;
            const std::size_t inserted_row_count = std::ranges::count_if(
                rows,
                [](const row_source& row)
                {
                    return row.is_inserted;
                }
            );
            out.keys.reserve(old_keys.size() + inserted_row_count * inserted_entry_size);
            out.items.reserve(old_items.size() + inserted_row_count * inserted_entry_size);
            out.offsets.reserve(rows.size() + 1);
            out.offsets.push_back(0);
            for (const auto& row : rows)
            {
                if (row.is_inserted)
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
                }
                else
                {
                    const auto begin = static_cast<std::size_t>(old_offsets[row.old_row]);
                    const auto end = static_cast<std::size_t>(old_offsets[row.old_row + 1]);
                    append_entry<true>(out.keys, out.items, out.offsets, old_keys, old_items, begin, end);
                }
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
        if (get_arrow_proxy().offset() != 0)
        {
            throw std::logic_error("map_array::insert_value does not support sliced arrays");
        }
        if (!m_keys_sorted)
        {
            throw std::invalid_argument("Cannot mutate a map_array with unsorted keys");
        }

        auto old_keys = snapshot_array(make_array_view(*raw_keys_array()));
        auto old_items = snapshot_array(make_array_view(*raw_items_array()));

        std::vector<dynamic_value> inserted_keys;
        std::vector<dynamic_value> inserted_items;
        inserted_keys.reserve(value.size());
        inserted_items.reserve(value.size());
        for (const auto& entry : value)
        {
            inserted_keys.push_back(array_materialize_element(entry.first));
            inserted_items.push_back(array_materialize_element(entry.second));
        }

        const size_type old_size = size();
        const auto old_offsets = p_list_offsets;
        std::vector<row_source> rows;
        rows.reserve(old_size + count);
        for (size_type row = 0; row < index; ++row)
        {
            rows.emplace_back(false, row);
        }
        rows.insert(rows.end(), count, row_source(true, 0));
        for (size_type row = index; row < old_size; ++row)
        {
            rows.emplace_back(false, row);
        }

        auto rebuilt = rebuild_flat_entries(
            std::move(old_keys),
            std::move(old_items),
            old_offsets,
            rows,
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
        if (get_arrow_proxy().offset() != 0)
        {
            throw std::logic_error("map_array::erase_values does not support sliced arrays");
        }
        if (!m_keys_sorted)
        {
            throw std::invalid_argument("Cannot mutate a map_array with unsorted keys");
        }

        auto old_keys = snapshot_array(make_array_view(*raw_keys_array()));
        auto old_items = snapshot_array(make_array_view(*raw_items_array()));
        const size_type old_size = size();
        const auto old_offsets = p_list_offsets;

        std::vector<row_source> rows;
        rows.reserve(old_size - count);
        for (size_type row = 0; row < old_size; ++row)
        {
            if (row < index || row >= index + count)
            {
                rows.emplace_back(false, row);
            }
        }

        auto rebuilt = rebuild_flat_entries(std::move(old_keys), std::move(old_items), old_offsets, rows, {}, {}, 0);
        replace_contents(
            std::move(rebuilt.keys),
            std::move(rebuilt.items),
            std::move(rebuilt.offsets)
        );
        return std::next(value_begin(), static_cast<std::ptrdiff_t>(index));
    }
}
