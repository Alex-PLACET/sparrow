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

#include <climits>
#include <concepts>
#include <ranges>
#include <string>
#include <utility>

#include "sparrow/buffer/buffer.hpp"
#include "sparrow/buffer/buffer_view.hpp"
#include "sparrow/utils/contracts.hpp"
#include "sparrow/utils/mp_utils.hpp"

namespace sparrow
{
    template <class B, bool is_const>
    class bitset_iterator;

    template <class B>
    class bitset_reference;

    template <class T>
    concept random_access_range = std::ranges::random_access_range<T>;

    /**
     * @class dynamic_bitset_base
     *
     * Base class for dynamic_bitset and dynamic_bitset_view.
     * Both represent a dynamic size sequence of bits. The only
     * difference between dynamic_bitset and dynamic_bitset_view
     * is that the former holds and manages its memory while
     * the second does not.
     *
     * @tparam B the underlying storage
     */
    template <random_access_range B>
    class dynamic_bitset_base
    {
    public:

        using self_type = dynamic_bitset_base<B>;
        using storage_type = B;
        using block_type = typename storage_type::value_type;
        using value_type = bool;
        using reference = bitset_reference<self_type>;
        using const_reference = bool;
        using size_type = typename storage_type::size_type;
        using difference_type = typename storage_type::difference_type;
        using iterator = bitset_iterator<self_type, false>;
        using const_iterator = bitset_iterator<self_type, true>;

        constexpr size_type size() const noexcept;
        [[nodiscard]] constexpr bool empty() const noexcept;
        constexpr size_type null_count() const noexcept;

        constexpr bool test(size_type pos) const;
        constexpr void set(size_type pos, value_type value);

        constexpr const_reference at(size_type pos) const;
        constexpr reference at(size_type pos);

        constexpr reference operator[](size_type i);
        constexpr const_reference operator[](size_type i) const;

        constexpr block_type* data() noexcept;
        constexpr const block_type* data() const noexcept;
        constexpr size_type block_count() const noexcept;

        constexpr void swap(self_type&) noexcept;

        constexpr iterator begin();
        constexpr iterator end();

        constexpr const_iterator begin() const;
        constexpr const_iterator end() const;
        constexpr const_iterator cbegin() const;
        constexpr const_iterator cend() const;

        constexpr reference front();
        constexpr const_reference front() const;
        constexpr reference back();
        constexpr const_reference back() const;

        const storage_type& buffer() const noexcept
        {
            return m_buffer;
        }

        storage_type& buffer() noexcept
        {
            return m_buffer;
        }

    protected:

        constexpr dynamic_bitset_base(storage_type&& buffer, size_type size);
        constexpr dynamic_bitset_base(storage_type&& buffer, size_type size, size_type null_count);
        constexpr ~dynamic_bitset_base() = default;

        constexpr dynamic_bitset_base(const dynamic_bitset_base&) = default;
        constexpr dynamic_bitset_base(dynamic_bitset_base&&) noexcept = default;

        constexpr dynamic_bitset_base& operator=(const dynamic_bitset_base&) = default;
        constexpr dynamic_bitset_base& operator=(dynamic_bitset_base&&) noexcept = default;

        constexpr void resize(size_type n, value_type b = false);
        constexpr void clear() noexcept;

        constexpr iterator insert(const_iterator pos, value_type value);
        constexpr iterator insert(const_iterator pos, size_type count, value_type value);
        template <std::input_iterator InputIt>
        constexpr iterator insert(const_iterator pos, InputIt first, InputIt last);
        constexpr iterator insert(const_iterator pos, std::initializer_list<value_type> ilist);
        constexpr iterator emplace(const_iterator pos, value_type value);
        constexpr iterator erase(const_iterator pos);
        constexpr iterator erase(const_iterator first, const_iterator last);

        constexpr void push_back(value_type value);
        constexpr void pop_back();

        constexpr size_type compute_block_count(size_type bits_count) const noexcept;


    private:

        static constexpr std::size_t s_bits_per_block = sizeof(block_type) * CHAR_BIT;
        static constexpr size_type block_index(size_type pos) noexcept;
        static constexpr size_type bit_index(size_type pos) noexcept;
        static constexpr block_type bit_mask(size_type pos) noexcept;

        size_type count_non_null() const noexcept;
        constexpr size_type count_extra_bits() const noexcept;
        constexpr void zero_unused_bits();
        constexpr void update_null_count(bool old_value, bool new_value);

        storage_type m_buffer;
        size_type m_size;
        size_type m_null_count;

        friend class bitset_iterator<self_type, true>;
        friend class bitset_iterator<self_type, false>;
        friend class bitset_reference<self_type>;
    };

    /**
     * @class dynamic_bitset
     *
     * This class represents a dynamic size sequence of bits.
     *
     * @tparam T the integer type used to store the bits.
     */
    template <std::integral T>
    class dynamic_bitset : public dynamic_bitset_base<buffer<T>>
    {
    public:

        using base_type = dynamic_bitset_base<buffer<T>>;
        using storage_type = typename base_type::storage_type;
        using block_type = typename base_type::block_type;
        using value_type = typename base_type::value_type;
        using size_type = typename base_type::size_type;

        constexpr dynamic_bitset();
        constexpr explicit dynamic_bitset(size_type n);
        constexpr dynamic_bitset(size_type n, value_type v);
        constexpr dynamic_bitset(block_type* p, size_type n);
        constexpr dynamic_bitset(block_type* p, size_type n, size_type null_count);

        constexpr ~dynamic_bitset() = default;
        constexpr dynamic_bitset(const dynamic_bitset&) = default;
        constexpr dynamic_bitset(dynamic_bitset&&) noexcept = default;

        constexpr dynamic_bitset& operator=(const dynamic_bitset&) = default;
        constexpr dynamic_bitset& operator=(dynamic_bitset&&) noexcept = default;

        using base_type::clear;
        using base_type::emplace;
        using base_type::erase;
        using base_type::insert;
        using base_type::pop_back;
        using base_type::push_back;
        using base_type::resize;
    };

    /**
     * @class dynamic_bitset_view
     *
     * This class represents a view to a dynamic size sequence of bits.
     *
     * @tparam T the integer type used to store the bits.
     */
    template <std::integral T>
    class dynamic_bitset_view : public dynamic_bitset_base<buffer_view<T>>
    {
    public:

        using base_type = dynamic_bitset_base<buffer_view<T>>;
        using storage_type = typename base_type::storage_type;
        using block_type = typename base_type::block_type;
        using size_type = typename base_type::size_type;

        constexpr dynamic_bitset_view(block_type* p, size_type n);
        constexpr dynamic_bitset_view(block_type* p, size_type n, size_type null_count);
        constexpr ~dynamic_bitset_view() = default;

        constexpr dynamic_bitset_view(const dynamic_bitset_view&) = default;
        constexpr dynamic_bitset_view(dynamic_bitset_view&&) noexcept = default;

        constexpr dynamic_bitset_view& operator=(const dynamic_bitset_view&) = default;
        constexpr dynamic_bitset_view& operator=(dynamic_bitset_view&&) noexcept = default;
    };

    /**
     * @class bitset_reference
     *
     * Reference proxy used by the bitset_iterator class
     * to make it possible to assign a bit of a bitset as a regular reference.
     *
     * @tparam B the dynamic_bitset containing the bit this class refers to.
     */
    template <class B>
    class bitset_reference
    {
    public:

        using self_type = bitset_reference<B>;

        constexpr bitset_reference(const bitset_reference&) noexcept = default;
        constexpr bitset_reference(bitset_reference&&) noexcept = default;

        constexpr self_type& operator=(const self_type&) noexcept;
        constexpr self_type& operator=(self_type&&) noexcept;
        constexpr self_type& operator=(bool) noexcept;

        constexpr operator bool() const noexcept;

        constexpr bool operator~() const noexcept;

        constexpr self_type& operator&=(bool) noexcept;
        constexpr self_type& operator|=(bool) noexcept;
        constexpr self_type& operator^=(bool) noexcept;

    private:

        using block_type = typename B::block_type;
        using bitset_type = B;

        bitset_reference(bitset_type& bitset, block_type& block, block_type mask);

        constexpr void assign(bool) noexcept;
        constexpr void set() noexcept;
        constexpr void reset() noexcept;

        bitset_type& m_bitset;
        block_type& m_block;
        block_type m_mask;

        friend class bitset_iterator<B, false>;
        template <random_access_range RAR>
        friend class dynamic_bitset_base;
    };

    template <class B1, class B2>
    bool operator==(const bitset_reference<B1>& lhs, const bitset_reference<B2>& rhs);

    template <class B>
    bool operator==(const bitset_reference<B>& lhs, bool rhs);

    /**
     * @class bitset_iterator
     *
     * Iterator used to iterate over the bits of a dynamic
     * bitset as if they were addressable values.
     *
     * @tparam B the dynamic bitset this iterator operates on
     * @tparam is_const a boolean indicating whether this is
     * a const iterator.
     */
    template <class B, bool is_const>
    class bitset_iterator : public iterator_base<
                                bitset_iterator<B, is_const>,
                                mpl::constify_t<typename B::value_type, is_const>,
                                std::random_access_iterator_tag,
                                std::conditional_t<is_const, bool, bitset_reference<B>>>
    {
    public:

        using self_type = bitset_iterator<B, is_const>;
        using base_type = iterator_base<
            self_type,
            mpl::constify_t<typename B::value_type, is_const>,
            std::contiguous_iterator_tag,
            std::conditional_t<is_const, bool, bitset_reference<B>>>;
        using reference = typename base_type::reference;
        using difference_type = typename base_type::difference_type;

        using block_type = mpl::constify_t<typename B::block_type, is_const>;
        using bitset_type = mpl::constify_t<B, is_const>;
        using size_type = typename B::size_type;

        constexpr bitset_iterator() noexcept = default;
        constexpr bitset_iterator(bitset_type* bitset, block_type* block, size_type index);

    private:

        constexpr reference dereference() const noexcept;
        constexpr void increment();
        constexpr void decrement();
        constexpr void advance(difference_type n);
        constexpr difference_type distance_to(const self_type& rhs) const noexcept;
        constexpr bool equal(const self_type& rhs) const noexcept;
        constexpr bool less_than(const self_type& rhs) const noexcept;

        constexpr bool is_first_bit_of_block(size_type index) const noexcept;
        constexpr difference_type distance_to_begin() const;

        bitset_type* p_bitset = nullptr;
        block_type* p_block = nullptr;
        // m_index is block-local index.
        // Invariant: m_index < bitset_type::s_bits_per_block
        size_type m_index;

        friend class iterator_access;
    };

    /**************************************
     * dynamic_bitset_base implementation *
     **************************************/

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::size() const noexcept -> size_type
    {
        return m_size;
    }

    template <random_access_range B>
    constexpr bool dynamic_bitset_base<B>::empty() const noexcept
    {
        return m_size == 0;
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::null_count() const noexcept -> size_type
    {
        return m_null_count;
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::operator[](size_type pos) -> reference
    {
        SPARROW_ASSERT_TRUE(pos < size());
        return reference(*this, m_buffer.data()[block_index(pos)], bit_mask(pos));
    }

    template <random_access_range B>
    constexpr bool dynamic_bitset_base<B>::operator[](size_type pos) const
    {
        return test(pos);
    }

    template <random_access_range B>
    constexpr bool dynamic_bitset_base<B>::test(size_type pos) const
    {
        SPARROW_ASSERT_TRUE(pos < size());
        return !m_null_count || m_buffer.data()[block_index(pos)] & bit_mask(pos);
    }

    template <random_access_range B>
    constexpr void dynamic_bitset_base<B>::set(size_type pos, value_type value)
    {
        SPARROW_ASSERT_TRUE(pos < size());
        block_type& block = m_buffer.data()[block_index(pos)];
        const bool old_value = block & bit_mask(pos);
        if (value)
        {
            block |= bit_mask(pos);
        }
        else
        {
            block &= block_type(~bit_mask(pos));
        }
        update_null_count(old_value, value);
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::data() noexcept -> block_type*
    {
        return m_buffer.data();
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::data() const noexcept -> const block_type*
    {
        return m_buffer.data();
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::block_count() const noexcept -> size_type
    {
        return m_buffer.size();
    }

    template <random_access_range B>
    constexpr void dynamic_bitset_base<B>::swap(self_type& rhs) noexcept
    {
        using std::swap;
        swap(m_buffer, rhs.m_buffer);
        swap(m_size, rhs.m_size);
        swap(m_null_count, rhs.m_null_count);
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::begin() -> iterator
    {
        return iterator(this, data(), 0u);
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::end() -> iterator
    {
        block_type* block = m_buffer.size() ? data() + m_buffer.size() - 1 : data();
        return iterator(this, block, size() % s_bits_per_block);
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::begin() const -> const_iterator
    {
        return cbegin();
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::end() const -> const_iterator
    {
        return cend();
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::cbegin() const -> const_iterator
    {
        return const_iterator(this, data(), 0u);
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::cend() const -> const_iterator
    {
        const block_type* block = m_buffer.size() ? data() + m_buffer.size() - 1 : data();
        return const_iterator(this, block, size() % s_bits_per_block);
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::at(size_type pos) -> reference
    {
        if (pos >= size())
        {
            throw std::out_of_range(
                "dynamic_bitset_base::at: index out of range for dynamic_bitset_base of size"
                + std::to_string(size()) + " at index " + std::to_string(pos)
            );
        }
        return (*this)[pos];
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::at(size_type pos) const -> const_reference
    {
        if (pos >= size())
        {
            throw std::out_of_range(
                "dynamic_bitset_base::at: index out of range for dynamic_bitset_base of size"
                + std::to_string(size()) + " at index " + std::to_string(pos)
            );
        }
        return test(pos);
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::front() -> reference
    {
        SPARROW_ASSERT_TRUE(size() >= 1);
        return (*this)[0];
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::front() const -> const_reference
    {
        SPARROW_ASSERT_TRUE(size() >= 1);
        return (*this)[0];
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::back() -> reference
    {
        SPARROW_ASSERT_TRUE(size() >= 1);
        return (*this)[size() - 1];
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::back() const -> const_reference
    {
        SPARROW_ASSERT_TRUE(size() >= 1);
        return (*this)[size() - 1];
    }

    template <random_access_range B>
    constexpr dynamic_bitset_base<B>::dynamic_bitset_base(storage_type&& buf, size_type size)
        : m_buffer(std::move(buf))
        , m_size(size)
        , m_null_count(m_size - count_non_null())
    {
        if constexpr (!std::is_const_v<block_type>)
        {
            zero_unused_bits();
        }
    }

    template <random_access_range B>
    constexpr dynamic_bitset_base<B>::dynamic_bitset_base(storage_type&& buf, size_type size, size_type null_count)
        : m_buffer(std::move(buf))
        , m_size(size)
        , m_null_count(null_count)
    {
        if constexpr (!std::is_const_v<block_type>)
        {
            zero_unused_bits();
            SPARROW_ASSERT_TRUE(m_null_count == m_size - count_non_null());
        }
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::compute_block_count(size_type bits_count) const noexcept -> size_type
    {
        return bits_count / s_bits_per_block + static_cast<size_type>(bits_count % s_bits_per_block != 0);
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::block_index(size_type pos) noexcept -> size_type
    {
        return pos / s_bits_per_block;
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::bit_index(size_type pos) noexcept -> size_type
    {
        return pos % s_bits_per_block;
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::bit_mask(size_type pos) noexcept -> block_type
    {
        const size_type bit = bit_index(pos);
        return static_cast<block_type>(block_type(1) << bit);
    }

    template <random_access_range B>
    auto dynamic_bitset_base<B>::count_non_null() const noexcept -> size_type
    {
        if (m_buffer.empty())
        {
            return 0u;
        }

        // Number of bits set to 1 in i for i from 0 to 255.
        // This can be seen as a mapping "uint8_t -> number of non null bits"
        static constexpr unsigned char table[] = {
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
        };
        // This methods sums up the number of non null bits per block of 8 bits.
        size_type res = 0;
        const unsigned char* p = reinterpret_cast<const unsigned char*>(m_buffer.data());
        const size_type length = m_buffer.size() * sizeof(block_type);
        for (size_type i = 0; i < length; ++i, ++p)
        {
            res += table[*p];
        }
        return res;
    }

    template <random_access_range B>
    constexpr auto dynamic_bitset_base<B>::count_extra_bits() const noexcept -> size_type
    {
        return bit_index(size());
    }

    template <random_access_range B>
    constexpr void dynamic_bitset_base<B>::zero_unused_bits()
    {
        const size_type extra_bits = count_extra_bits();
        if (extra_bits != 0)
        {
            m_buffer.back() &= block_type(~(~block_type(0) << extra_bits));
        }
    }

    template <random_access_range B>
    constexpr void dynamic_bitset_base<B>::update_null_count(bool old_value, bool new_value)
    {
        if (new_value && !old_value)
        {
            --m_null_count;
        }
        else if (!new_value && old_value)
        {
            ++m_null_count;
        }
    }

    template <random_access_range B>
    constexpr void dynamic_bitset_base<B>::resize(size_type n, value_type b)
    {
        const size_type old_block_count = m_buffer.size();
        const size_type new_block_count = compute_block_count(n);
        const block_type value = b ? block_type(~block_type(0)) : block_type(0);

        if (new_block_count != old_block_count)
        {
            m_buffer.resize(new_block_count, value);
        }

        if (b && n > m_size)
        {
            const size_type extra_bits = count_extra_bits();
            if (extra_bits > 0)
            {
                m_buffer.data()[old_block_count - 1] |= static_cast<block_type>(value << extra_bits);
            }
        }

        m_size = n;
        m_null_count = m_size - count_non_null();
        zero_unused_bits();
    }

    template <random_access_range B>
    constexpr void dynamic_bitset_base<B>::clear() noexcept
    {
        m_buffer.clear();
        m_size = 0;
        m_null_count = 0;
    }

    template <random_access_range B>
    constexpr dynamic_bitset_base<B>::iterator
    dynamic_bitset_base<B>::insert(const_iterator pos, value_type value)
    {
        return insert(pos, 1, value);
    }

    template <random_access_range B>
    constexpr dynamic_bitset_base<B>::iterator
    dynamic_bitset_base<B>::insert(const_iterator pos, size_type count, value_type value)
    {
        SPARROW_ASSERT_TRUE(cbegin() <= pos);
        SPARROW_ASSERT_TRUE(pos <= cend());
        const auto index = static_cast<size_type>(std::distance(cbegin(), pos));
        const size_type old_size = size();
        const size_type new_size = old_size + count;

        // TODO: The current implementation is not efficient. It can be improved.

        resize(new_size);

        for (size_type i = old_size + count - 1; i >= index + count; --i)
        {
            set(i, test(i - count));
        }

        for (size_type i = 0; i < count; ++i)
        {
            set(index + i, value);
        }

        auto block = data() + block_index(index);
        return iterator(this, block, bit_index(index));
    }

    template <random_access_range B>
    template <std::input_iterator InputIt>
    constexpr dynamic_bitset_base<B>::iterator
    dynamic_bitset_base<B>::insert(const_iterator pos, InputIt first, InputIt last)
    {
        SPARROW_ASSERT_TRUE(cbegin() <= pos);
        SPARROW_ASSERT_TRUE(pos <= cend());
        const auto index = static_cast<size_type>(std::distance(cbegin(), pos));
        const size_type old_size = size();
        const size_type count = static_cast<size_type>(std::distance(first, last));
        const size_type new_size = old_size + count;

        resize(new_size);

        // TODO: The current implementation is not efficient. It can be improved.

        for (size_type i = old_size + count - 1; i >= index + count; --i)
        {
            set(i, test(i - count));
        }

        for (size_type i = 0; i < count; ++i)
        {
            set(index + i, *first++);
        }

        auto block = data() + block_index(index);
        return iterator(this, block, bit_index(index));
    }

    template <random_access_range B>
    constexpr dynamic_bitset_base<B>::iterator
    dynamic_bitset_base<B>::emplace(const_iterator pos, value_type value)
    {
        return insert(pos, value);
    }

    template <random_access_range B>
    constexpr dynamic_bitset_base<B>::iterator dynamic_bitset_base<B>::erase(const_iterator pos)
    {
        SPARROW_ASSERT_TRUE(cbegin() <= pos);
        SPARROW_ASSERT_TRUE(pos < cend());

        return erase(pos, pos + 1);
    }

    template <random_access_range B>
    constexpr dynamic_bitset_base<B>::iterator
    dynamic_bitset_base<B>::erase(const_iterator first, const_iterator last)
    {
        SPARROW_ASSERT_TRUE(cbegin() <= first);
        SPARROW_ASSERT_TRUE(first <= last);
        SPARROW_ASSERT_TRUE(last <= cend());

        const auto first_index = static_cast<size_type>(std::distance(cbegin(), first));

        if (last == cend())
        {
            resize(first_index);
            return end();
        }

        // TODO: The current implementation is not efficient. It can be improved.

        const auto last_index = static_cast<size_type>(std::distance(cbegin(), last));
        const size_type count = last_index - first_index;

        const size_type bit_to_move = size() - last_index;
        for (size_type i = 0; i < bit_to_move; ++i)
        {
            set(first_index + i, test(last_index + i));
        }

        resize(size() - count);

        auto block = data() + block_index(first_index);
        return iterator(this, block, bit_index(first_index));
    }

    template <random_access_range B>
    constexpr void dynamic_bitset_base<B>::push_back(value_type value)
    {
        resize(size() + 1, value);
    }

    template <random_access_range B>
    constexpr void dynamic_bitset_base<B>::pop_back()
    {
        resize(size() - 1);
    }

    /*********************************
     * dynamic_bitset implementation *
     *********************************/

    template <std::integral T>
    constexpr dynamic_bitset<T>::dynamic_bitset()
        : base_type(storage_type(), 0u)
    {
    }

    template <std::integral T>
    constexpr dynamic_bitset<T>::dynamic_bitset(size_type n)
        : dynamic_bitset(n, false)
    {
    }

    template <std::integral T>
    constexpr dynamic_bitset<T>::dynamic_bitset(size_type n, value_type value)
        : base_type(
              storage_type(this->compute_block_count(n), value ? block_type(~block_type(0)) : block_type(0)),
              n,
              value ? 0u : n
          )
    {
    }

    template <std::integral T>
    constexpr dynamic_bitset<T>::dynamic_bitset(block_type* p, size_type n)
        : base_type(storage_type(p, this->compute_block_count(n)), n)
    {
    }

    template <std::integral T>
    constexpr dynamic_bitset<T>::dynamic_bitset(block_type* p, size_type n, size_type null_count)
        : base_type(storage_type(p, this->compute_block_count(n)), n, null_count)
    {
    }

    /*************************************
     * dyamic_bitset_view implementation *
     *************************************/

    template <std::integral T>
    constexpr dynamic_bitset_view<T>::dynamic_bitset_view(block_type* p, size_type n)
        : base_type(storage_type(p, this->compute_block_count(n)), n)
    {
    }

    template <std::integral T>
    constexpr dynamic_bitset_view<T>::dynamic_bitset_view(block_type* p, size_type n, size_type null_count)
        : base_type(storage_type(p, this->compute_block_count(n)), n, null_count)
    {
    }

    /***********************************
     * bitset_reference implementation *
     ***********************************/

    template <class B>
    constexpr auto bitset_reference<B>::operator=(const self_type& rhs) noexcept -> self_type&
    {
        assign(rhs);
        return *this;
    }

    template <class B>
    constexpr auto bitset_reference<B>::operator=(self_type&& rhs) noexcept -> self_type&
    {
        assign(rhs);
        return *this;
    }

    template <class B>
    constexpr auto bitset_reference<B>::operator=(bool rhs) noexcept -> self_type&
    {
        assign(rhs);
        return *this;
    }

    template <class B>
    constexpr bitset_reference<B>::operator bool() const noexcept
    {
        return (m_block & m_mask) != 0;
    }

    template <class B>
    constexpr bool bitset_reference<B>::operator~() const noexcept
    {
        return (m_block & m_mask) == 0;
    }

    template <class B>
    constexpr auto bitset_reference<B>::operator&=(bool rhs) noexcept -> self_type&
    {
        if (!rhs)
        {
            reset();
        }
        return *this;
    }

    template <class B>
    constexpr auto bitset_reference<B>::operator|=(bool rhs) noexcept -> self_type&
    {
        if (rhs)
        {
            set();
        }
        return *this;
    }

    template <class B>
    constexpr auto bitset_reference<B>::operator^=(bool rhs) noexcept -> self_type&
    {
        if (rhs)
        {
            bool old_value = m_block & m_mask;
            m_block ^= m_mask;
            m_bitset.update_null_count(old_value, !old_value);
        }
        return *this;
    }

    template <class B>
    bitset_reference<B>::bitset_reference(bitset_type& bitset, block_type& block, block_type mask)
        : m_bitset(bitset)
        , m_block(block)
        , m_mask(mask)
    {
    }

    template <class B>
    constexpr void bitset_reference<B>::assign(bool rhs) noexcept
    {
        rhs ? set() : reset();
    }

    template <class B>
    constexpr void bitset_reference<B>::set() noexcept
    {
        bool old_value = m_block & m_mask;
        m_block |= m_mask;
        m_bitset.update_null_count(old_value, m_block & m_mask);
    }

    template <class B>
    constexpr void bitset_reference<B>::reset() noexcept
    {
        bool old_value = m_block & m_mask;
        m_block &= ~m_mask;
        m_bitset.update_null_count(old_value, m_block & m_mask);
    }

    template <class B1, class B2>
    bool operator==(const bitset_reference<B1>& lhs, const bitset_reference<B2>& rhs)
    {
        return bool(lhs) == bool(rhs);
    }

    template <class B>
    bool operator==(const bitset_reference<B>& lhs, bool rhs)
    {
        return bool(lhs) == rhs;
    }

    /**********************************
     * bitset_iterator implementation *
     **********************************/

    template <class B, bool is_const>
    constexpr bitset_iterator<B, is_const>::bitset_iterator(bitset_type* bitset, block_type* block, size_type index)
        : p_bitset(bitset)
        , p_block(block)
        , m_index(index)
    {
        SPARROW_ASSERT_TRUE(m_index < bitset_type::s_bits_per_block);
    }

    template <class B, bool is_const>
    constexpr auto bitset_iterator<B, is_const>::dereference() const noexcept -> reference
    {
        if constexpr (is_const)
        {
            return (*p_block) & bitset_type::bit_mask(m_index);
        }
        else
        {
            return bitset_reference<B>(*p_bitset, *p_block, bitset_type::bit_mask(m_index));
        }
    }

    template <class B, bool is_const>
    constexpr void bitset_iterator<B, is_const>::increment()
    {
        ++m_index;
        // If we have reached next block
        if (is_first_bit_of_block(m_index))
        {
            ++p_block;
            m_index = 0u;
        }
        SPARROW_ASSERT_TRUE(m_index < bitset_type::s_bits_per_block);
    }

    template <class B, bool is_const>
    constexpr void bitset_iterator<B, is_const>::decrement()
    {
        // decreasing moves to previous block
        if (is_first_bit_of_block(m_index))
        {
            --p_block;
            m_index = sizeof(block_type);
        }
        else
        {
            --m_index;
        }
        SPARROW_ASSERT_TRUE(m_index < bitset_type::s_bits_per_block);
    }

    template <class B, bool is_const>
    constexpr void bitset_iterator<B, is_const>::advance(difference_type n)
    {
        if (n >= 0)
        {
            if (std::cmp_less(n, bitset_type::s_bits_per_block - m_index))
            {
                m_index += static_cast<size_type>(n);
            }
            else
            {
                const size_type to_next_block = bitset_type::s_bits_per_block - m_index;
                n -= static_cast<difference_type>(to_next_block);
                const size_type block_n = static_cast<size_type>(n) / bitset_type::s_bits_per_block;
                p_block += block_n + 1;
                n -= static_cast<difference_type>(block_n * bitset_type::s_bits_per_block);
                m_index = static_cast<size_type>(n);
            }
        }
        else
        {
            size_type mn = static_cast<size_type>(-n);
            if (m_index >= mn)
            {
                m_index -= mn;
            }
            else
            {
                const size_type block_n = mn / bitset_type::s_bits_per_block;
                p_block -= block_n;
                mn -= block_n * bitset_type::s_bits_per_block;
                if (m_index >= mn)
                {
                    m_index -= mn;
                }
                else
                {
                    mn -= m_index;
                    --p_block;
                    m_index = bitset_type::s_bits_per_block - mn;
                }
            }
        }
        SPARROW_ASSERT_TRUE(m_index < bitset_type::s_bits_per_block);
    }

    template <class B, bool is_const>
    constexpr auto
    bitset_iterator<B, is_const>::distance_to(const self_type& rhs) const noexcept -> difference_type
    {
        if (p_block == rhs.p_block)
        {
            return static_cast<difference_type>(rhs.m_index - m_index);
        }
        else
        {
            const auto dist1 = distance_to_begin();
            const auto dist2 = rhs.distance_to_begin();
            return dist2 - dist1;
        }
    }

    template <class B, bool is_const>
    constexpr bool bitset_iterator<B, is_const>::equal(const self_type& rhs) const noexcept
    {
        return p_block == rhs.p_block && m_index == rhs.m_index;
    }

    template <class B, bool is_const>
    constexpr bool bitset_iterator<B, is_const>::less_than(const self_type& rhs) const noexcept
    {
        return (p_block < rhs.p_block) || (p_block == rhs.p_block && m_index < rhs.m_index);
    }

    template <class B, bool is_const>
    constexpr bool bitset_iterator<B, is_const>::is_first_bit_of_block(size_type index) const noexcept
    {
        return index % bitset_type::s_bits_per_block == 0;
    }

    template <class B, bool is_const>
    constexpr auto bitset_iterator<B, is_const>::distance_to_begin() const -> difference_type
    {
        const difference_type distance = p_block - p_bitset->begin().p_block;
        SPARROW_ASSERT_TRUE(distance >= 0);
        return static_cast<difference_type>(bitset_type::s_bits_per_block) * distance
               + static_cast<difference_type>(m_index);
    }
}
