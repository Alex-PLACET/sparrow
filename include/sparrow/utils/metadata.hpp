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

#include <concepts>
#include <numeric>
#include <ranges>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace sparrow
{
    using metadata_key = std::string_view;
    using metadata_value = std::string_view;
    using metadata_pair = std::pair<metadata_key, metadata_value>;

    template <typename T>
    concept output_metadata_container = std::ranges::output_range<T, metadata_pair>;

    // Helper function to extract an int32 from a char buffer
    int32_t extract_int32(const char*& ptr)
    {
        return *reinterpret_cast<const int32_t*>(ptr++);
    }

    // Custom view to lazily extract key/value pairs from the buffer
    template <typename Iter>
    class KeyValueView : public std::ranges::view_interface<KeyValueView<Iter>>
    {
    public:

        KeyValueView() = default;

        KeyValueView(Iter begin)
            : m_begin(begin)
            , m_num_pairs(extract_int32(&*m_begin))
        {
        }

        auto begin()
        {
            return iterator(*this, 0);
        }

        auto end()
        {
            return iterator(*this, m_num_pairs);
        }

    private:

        class iterator
        {
        public:

            using iterator_category = std::input_iterator_tag;
            using value_type = std::pair<std::string_view, std::string_view>;
            using difference_type = std::ptrdiff_t;
            using pointer = value_type*;
            using reference = value_type&;

            iterator() = default;

            iterator(KeyValueView& parent, int32_t index)
                : m_parent(parent)
                , m_index(index)
            {
                if (m_index < m_parent.m_num_pairs)
                {
                    m_current = m_parent.m_current;
                    extract_key_value();
                }
            }

            value_type operator*() const
            {
                return {m_key, m_value};
            }

            iterator& operator++()
            {
                ++m_index;
                if (m_index < m_parent.m_num_pairs)
                {
                    m_current = m_parent.m_current;
                    extract_key_value();
                }
                return *this;
            }

            friend bool operator==(const iterator& lhs, const iterator& rhs)
            {
                return lhs.m_index == rhs.m_index;
            }

            friend bool operator!=(const iterator& lhs, const iterator& rhs)
            {
                return !(lhs == rhs);
            }

        private:

            std::string_view extract_string_view()
            {
                const int32_t length = extract_int32(m_current);
                std::string_view str_view(m_current, length);
                m_current += length;
                return str_view;
            }

            void extract_key_value()
            {
                m_key = extract_string_view();
                m_value = extract_string_view();
            }

            KeyValueView& m_parent;
            int32_t m_index;
            const char* m_current;
            std::string_view m_key;
            std::string_view m_value;
        };

        Iter m_begin;
        int32_t m_num_pairs = 0;
    };

    // Helper function to create the KeyValueView
    auto create_key_value_view(const std::vector<char>& buffer)
    {
        return KeyValueView(buffer.begin());
    }

    template <std::ranges::contiguous_range T>
        requires std::convertible_to<std::ranges::range_value_t<T>, char>
    auto get_key_values_from_metadata(const T& metadata)
    {
        const char* metadata_ptr = metadata.data();
        const int32_t number_of_key_values = reinterpret_cast<const int32_t*>(metadata_ptr)[0];
        metadata_ptr += sizeof(int32_t);

        return metadata
               | std::ranges::views::transform(
                   [&metadata_ptr, number_of_key_values](const auto&)
                   {
                       const int32_t key_byte_length = reinterpret_cast<const int32_t*>(metadata_ptr)[0];
                       metadata_ptr += sizeof(int32_t);
                       const std::string_view key = std::string_view(metadata_ptr, key_byte_length);
                       metadata_ptr += key_byte_length;
                       const int32_t value_byte_length = reinterpret_cast<const int32_t*>(metadata_ptr)[0];
                       metadata_ptr += sizeof(int32_t);
                       const std::string_view value = std::string_view(metadata_ptr, value_byte_length);
                       metadata_ptr += value_byte_length;
                       return std::make_pair(key, value);
                   }
               );
    }

    template <typename T>
    concept input_metadata_container = std::ranges::input_range<T>
                                       && std::same_as<std::ranges::range_value_t<T>, metadata_pair>;

    std::vector<char> get_metadata_from_key_values(const input_metadata_container auto& metadata)
    {
        const auto number_of_key_values = static_cast<int32_t>(metadata.size());
        const int32_t metadata_size = std::accumulate(
            metadata.cbegin(),
            metadata.cend(),
            0,
            [](int32_t acc, const auto& pair)
            {
                return acc + sizeof(int32_t)  // byte length of key
                       + pair.first.size()    // number of bytes of key
                       + sizeof(int32_t)      // byte length of value
                       + pair.second.size();  // number of bytes of value
            }
        );
        const int32_t total_size = sizeof(int32_t) + metadata_size;
        std::vector<char> metadata_buffer(total_size, '\0');
        char* metadata_ptr = metadata_buffer.data();
        reinterpret_cast<int32_t*>(metadata_ptr)[0] = number_of_key_values;
        metadata_ptr += sizeof(int32_t);
        for (const auto& [key, value] : metadata)
        {
            SPARROW_ASSERT_TRUE(std::cmp_less(key.size(), std::numeric_limits<int32_t>::max()));
            SPARROW_ASSERT_TRUE(std::cmp_less(value.size(), std::numeric_limits<int32_t>::max()));
            reinterpret_cast<int32_t*>(metadata_ptr)[0] = static_cast<int32_t>(key.size());
            metadata_ptr += sizeof(int32_t);
            std::ranges::copy(key, metadata_ptr);
            metadata_ptr += key.size();
            reinterpret_cast<int32_t*>(metadata_ptr)[0] = static_cast<int32_t>(value.size());
            metadata_ptr += sizeof(int32_t);
            std::ranges::copy(value, metadata_ptr);
            metadata_ptr += value.size();
        }
        return metadata_buffer;
    }
}
