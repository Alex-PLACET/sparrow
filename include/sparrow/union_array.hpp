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

#include <array>
#include <limits>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include "sparrow/array_api.hpp"
#include "sparrow/arrow_interface/arrow_flag_utils.hpp"
#include "sparrow/buffer/buffer_adaptor.hpp"
#include "sparrow/config/config.hpp"
#include "sparrow/layout/array_access.hpp"
#include "sparrow/layout/layout_utils.hpp"
#include "sparrow/layout/nested_value_types.hpp"
#include "sparrow/utils/contracts.hpp"
#include "sparrow/utils/crtp_base.hpp"
#include "sparrow/utils/functor_index_iterator.hpp"
#include "sparrow/utils/memory.hpp"
#include "sparrow/utils/metadata.hpp"
#include "sparrow/utils/mp_utils.hpp"
#include "sparrow/utils/nullable.hpp"

namespace sparrow
{
    class dense_union_array;
    class sparse_union_array;

    namespace detail
    {
        template <>
        struct get_data_type_from_array<sparrow::dense_union_array>
        {
            [[nodiscard]] static constexpr sparrow::data_type get()
            {
                return sparrow::data_type::DENSE_UNION;
            }
        };

        template <>
        struct get_data_type_from_array<sparrow::sparse_union_array>
        {
            [[nodiscard]] static constexpr sparrow::data_type get()
            {
                return sparrow::data_type::SPARSE_UNION;
            }
        };

        SPARROW_API void
        validate_union_child_insert_value(const array& child, const array_traits::value_type& value);

        SPARROW_API void validate_union_child_erase(const array& child);

        SPARROW_API void insert_union_child_value(
            array& child,
            std::size_t pos,
            const array_traits::value_type& value,
            std::size_t count
        );

        [[nodiscard]] SPARROW_API array_traits::value_type
        make_union_child_default_value(const array& child, bool is_valid);
    }

    /**
     * @brief Type trait to check if a type is a dense_union_array.
     *
     * @tparam T Type to check
     */
    template <class T>
    constexpr bool is_dense_union_array_v = std::same_as<T, dense_union_array>;

    /**
     * @brief Type trait to check if a type is a sparse_union_array.
     *
     * @tparam T Type to check
     */
    template <class T>
    constexpr bool is_sparse_union_array_v = std::same_as<T, sparse_union_array>;

    /**
     * @brief CRTP base class providing shared functionality for union array implementations.
     *
     * This class implements the common interface and logic shared between dense and sparse
     * union arrays. Union arrays can store values of different types in a single array,
     * with each element having an associated type ID that indicates which child array
     * contains the actual value.
     *
     * Key features:
     * - Type-safe heterogeneous storage
     * - Efficient type dispatch using type ID mapping
     * - STL-compatible container interface
     * - Support for both dense and sparse union layouts
     * - Arrow format compatibility
     *
     * The union array consists of:
     * - Type ID buffer: Maps each element to its corresponding child array
     * - Child arrays: Store the actual values for each supported type
     * - Type ID mapping: Translates type IDs to child array indices
     *
     * @tparam DERIVED The derived union array type (dense_union_array or sparse_union_array)
     *
     * @pre DERIVED must inherit from this class (CRTP pattern)
     * @pre DERIVED must implement element_offset() method
     * @post Provides complete union array interface
     * @post Maintains Arrow union format compatibility
     * @post Thread-safe for read operations
     *
     * @code{.cpp}
     * // Union arrays can store different types
     * dense_union_array union_arr = create_union_with_int_and_string();
     *
     * // Access elements (type is determined at runtime)
     * auto elem = union_arr[0];
     * if (elem.has_value()) {
     *     // elem could be int or string depending on type ID
     * }
     *
     * // Iteration over heterogeneous elements
     * for (const auto& element : union_arr) {
     *     // Process element of unknown type
     * }
     * @endcode
     */
    template <class DERIVED>
    class union_array_crtp_base : public crtp_base<DERIVED>
    {
    public:

        using self_type = union_array_crtp_base<DERIVED>;
        using derived_type = DERIVED;
        using inner_value_type = array_traits::inner_value_type;
        using value_type = array_traits::const_reference;
        using const_reference = array_traits::const_reference;
        using functor_type = detail::layout_bracket_functor<derived_type, value_type>;
        using const_functor_type = detail::layout_bracket_functor<const derived_type, value_type>;
        using iterator = functor_index_iterator<functor_type>;
        using const_iterator = functor_index_iterator<const_functor_type>;
        using const_reverse_iterator = std::reverse_iterator<const_iterator>;
        using size_type = std::size_t;

        using type_id_buffer_type = u8_buffer<std::uint8_t>;

        struct typed_value
        {
            std::uint8_t type_id = 0;
            array_traits::value_type value;

            typed_value() = default;

            constexpr typed_value(std::uint8_t type_id_arg, array_traits::value_type value_arg)
                : type_id(type_id_arg)
                , value(std::move(value_arg))
            {
            }

            template <class T, mpl::boolean_like B>
            constexpr typed_value(std::uint8_t type_id_arg, nullable<T, B> value_arg)
                : typed_value(type_id_arg, array_traits::value_type(std::move(value_arg)))
            {
            }
        };

        /**
         * @brief Gets the optional name of the union array.
         *
         * @return Optional string view of the array name from Arrow schema
         *
         * @post Returns nullopt if no name is set
         * @post Returned string view remains valid while array exists
         */
        [[nodiscard]] constexpr std::optional<std::string_view> name() const;

        /**
         * @brief Gets the metadata associated with the union array.
         *
         * @return Optional view of key-value metadata pairs from Arrow schema
         *
         * @post Returns nullopt if no metadata is set
         * @post Returned view remains valid while array exists
         */
        [[nodiscard]] SPARROW_CONSTEXPR_CLANG std::optional<key_value_view> metadata() const;

        /**
         * @brief Gets element at specified position with bounds checking.
         *
         * @param i Index of the element to access
         * @return Value from the appropriate child array
         *
         * @pre i must be < size()
         * @post Returns valid value from child array indicated by type ID
         * @post Value type depends on the type ID at position i
         *
         * @throws std::out_of_range if i >= size()
         */
        [[nodiscard]] SPARROW_CONSTEXPR_CLANG value_type at(size_type i) const;

        /**
         * @brief Gets element at specified position without bounds checking.
         *
         * @param i Index of the element to access
         * @return Value from the appropriate child array
         *
         * @pre i must be < size()
         * @post Returns value from child array indicated by type ID
         * @post Value type depends on the type ID at position i
         */
        [[nodiscard]] SPARROW_CONSTEXPR_CLANG value_type operator[](size_type i) const;

        /**
         * @brief Gets mutable element at specified position.
         *
         * @param i Index of the element to access
         * @return Value from the appropriate child array
         *
         * @pre i must be < size()
         * @post Returns value from child array indicated by type ID
         * @post Value type depends on the type ID at position i
         */
        [[nodiscard]] SPARROW_CONSTEXPR_CLANG value_type operator[](size_type i);

        /**
         * @brief Gets reference to the first element.
         *
         * @return Value from the appropriate child array for first element
         *
         * @pre Array must not be empty (!empty())
         * @post Returns valid value from child array
         * @post Equivalent to (*this)[0]
         */
        [[nodiscard]] SPARROW_CONSTEXPR_CLANG value_type front() const;

        /**
         * @brief Gets reference to the last element.
         *
         * @return Value from the appropriate child array for last element
         *
         * @pre Array must not be empty (!empty())
         * @post Returns valid value from child array
         * @post Equivalent to (*this)[size() - 1]
         */
        [[nodiscard]] SPARROW_CONSTEXPR_CLANG value_type back() const;

        /**
         * @brief Checks if the union array is empty.
         *
         * @return true if array contains no elements, false otherwise
         *
         * @post Return value equals (size() == 0)
         */
        [[nodiscard]] constexpr bool empty() const;

        /**
         * @brief Gets the number of elements in the union array.
         *
         * @return Number of elements in the array
         *
         * @post Returns non-negative value
         * @post Equals the number of type IDs in the type buffer
         */
        [[nodiscard]] constexpr size_type size() const;

        /**
         * @brief Gets iterator to the beginning of the array.
         *
         * @return Iterator pointing to the first element
         *
         * @post Iterator is valid for array traversal
         * @post For empty array, equals end()
         */
        [[nodiscard]] constexpr iterator begin();

        /**
         * @brief Gets iterator to the end of the array.
         *
         * @return Iterator pointing past the last element
         *
         * @post Iterator marks the end of the array range
         * @post Not dereferenceable
         */
        [[nodiscard]] constexpr iterator end();

        /**
         * @brief Gets const iterator to the beginning of the array.
         *
         * @return Const iterator pointing to the first element
         *
         * @post Iterator is valid for array traversal
         * @post For empty array, equals end()
         */
        [[nodiscard]] constexpr const_iterator begin() const;

        /**
         * @brief Gets const iterator to the end of the array.
         *
         * @return Const iterator pointing past the last element
         *
         * @post Iterator marks the end of the array range
         * @post Not dereferenceable
         */
        [[nodiscard]] constexpr const_iterator end() const;

        /**
         * @brief Gets const iterator to the beginning of the array.
         *
         * @return Const iterator pointing to the first element
         *
         * @post Iterator is valid for array traversal
         * @post Guarantees const iterator even for non-const array
         */
        [[nodiscard]] constexpr const_iterator cbegin() const;

        /**
         * @brief Gets const iterator to the end of the array.
         *
         * @return Const iterator pointing past the last element
         *
         * @post Iterator marks the end of the array range
         * @post Guarantees const iterator even for non-const array
         */
        [[nodiscard]] constexpr const_iterator cend() const;

        /**
         * @brief Gets reverse iterator to the beginning of reversed array.
         *
         * @return Const reverse iterator pointing to the last element
         *
         * @post Iterator is valid for reverse traversal
         * @post For empty array, equals rend()
         */
        [[nodiscard]] constexpr const_reverse_iterator rbegin() const;

        /**
         * @brief Gets reverse iterator to the end of reversed array.
         *
         * @return Const reverse iterator pointing before the first element
         *
         * @post Iterator marks the end of reverse traversal
         * @post Not dereferenceable
         */
        [[nodiscard]] constexpr const_reverse_iterator rend() const;

        /**
         * @brief Gets const reverse iterator to the beginning of reversed array.
         *
         * @return Const reverse iterator pointing to the last element
         *
         * @post Iterator is valid for reverse traversal
         * @post Guarantees const iterator even for non-const array
         */
        [[nodiscard]] constexpr const_reverse_iterator crbegin() const;

        /**
         * @brief Gets const reverse iterator to the end of reversed array.
         *
         * @return Const reverse iterator pointing before the first element
         *
         * @post Iterator marks the end of reverse traversal
         * @post Guarantees const iterator even for non-const array
         */
        [[nodiscard]] constexpr const_reverse_iterator crend() const;

        [[nodiscard]] iterator insert(const_iterator pos, const typed_value& value);

        [[nodiscard]] iterator insert(const_iterator pos, const typed_value& value, size_type count);

        template <std::input_iterator InputIt>
            requires std::convertible_to<typename std::iterator_traits<InputIt>::value_type, typed_value>
        [[nodiscard]] iterator insert(const_iterator pos, InputIt first, InputIt last);

        template <std::ranges::input_range R>
            requires std::convertible_to<std::ranges::range_value_t<R>, typed_value>
        [[nodiscard]] iterator insert(const_iterator pos, R&& range)
        {
            return insert(pos, std::ranges::begin(range), std::ranges::end(range));
        }

        [[nodiscard]] iterator erase(const_iterator pos);

        [[nodiscard]] iterator erase(const_iterator first, const_iterator last);

        void push_back(const typed_value& value);

        void pop_back();

        void resize(size_type new_length, const typed_value& value);

        void clear();

        /**
         * @brief Sets all null values to the specified value.
         *
         * This operation modifies the underlying data values but not the validity bitmap.
         * The bitmap remains unchanged, so the elements will still be considered null.
         * Only the actual stored values are replaced.
         *
         * @param value The value to assign to null elements
         *
         * @post All null positions contain the specified value
         * @post Validity bitmap remains unchanged
         * @post Elements are still logically null despite having a value
         */
        constexpr void zero_null_values(const inner_value_type& value)
        {
            sparrow::zero_null_values(*this, value);
        }

    protected:

        static constexpr size_t TYPE_ID_MAP_SIZE = 256;
        static constexpr size_type TYPE_IDS_BUFFER_INDEX = 0;
        static constexpr size_type INVALID_CHILD_INDEX = std::numeric_limits<size_type>::max();

        using type_id_map = std::array<size_type, TYPE_ID_MAP_SIZE>;

        /**
         * @brief Parses type ID mapping from Arrow format string.
         *
         * @param format_string Arrow format string containing type ID mappings
         * @return Array mapping type IDs to child indices
         *
         * @pre format_string must be valid Arrow union format ("+du:" or "+su:" prefix)
         * @post Returns valid type ID to child index mapping
         * @post Mapping is used for efficient type dispatch
         */
        static constexpr type_id_map parse_type_id_map(std::string_view format_string);

        /**
         * @brief Creates type ID mapping from child index to type ID mapping.
         *
         * @tparam R Range type containing type IDs
         * @param child_index_to_type_id Optional mapping from child index to type ID
         * @return Inverse mapping from type ID to child index
         *
         * @post If no mapping provided, uses identity mapping (0->0, 1->1, etc.)
         * @post Returned mapping enables efficient child array lookup
         */
        template <std::ranges::input_range R>
        static constexpr type_id_map
        type_id_map_from_child_to_type_id(const std::optional<R>& child_index_to_type_id);

        /**
         * @brief Creates Arrow format string for union arrays.
         *
         * @tparam R Range type for type ID mapping
         * @param dense Whether this is a dense union (true) or sparse (false)
         * @param n Number of child arrays
         * @param range Optional type ID mapping range
         * @return Arrow format string for the union
         *
         * @pre If range is provided, its size must equal n or be 0
         * @pre Range elements must be convertible to std::uint8_t
         * @post Returns valid Arrow format string ("+ud:" or "+us:" prefix)
         *
         * @throws std::invalid_argument if range size doesn't match n
         */
        template <std::ranges::input_range R>
            requires(std::convertible_to<std::ranges::range_value_t<R>, std::uint8_t>)
        static constexpr std::string
        make_format_string(bool dense, std::size_t n, const std::optional<R>& child_index_to_type_id);

        using children_type = std::vector<array>;

        /**
         * @brief Creates child array wrappers from Arrow proxy.
         *
         * @param proxy Arrow proxy containing child array data
         * @return Vector of child array wrappers
         *
         * @pre proxy must contain valid child array data
         * @post Returns valid children collection
         * @post Each child corresponds to a union member type
         */
        constexpr children_type make_children(arrow_proxy& proxy);

        void throw_if_sliced_for_mutation(const char* operation) const;

        [[nodiscard]] size_type child_index_from_type_id(std::uint8_t type_id) const;

        [[nodiscard]] bool child_is_nullable(size_type child_index) const;

        [[nodiscard]] const std::uint8_t* make_type_ids() const;

        void refresh_type_id_cache();

        [[nodiscard]] array& raw_child_wrapper(size_type child_index);

        [[nodiscard]] const array& child_wrapper(size_type child_index) const;

        void validate_child_insert_value(const array& child, const array_traits::value_type& value) const;

        void validate_child_erase(const array& child) const;

        void
        insert_child_value(array& child, size_type pos, const array_traits::value_type& value, size_type count) const;

        [[nodiscard]] array_traits::value_type make_inactive_child_value(size_type child_index) const;

        /**
         * @brief Protected constructor from Arrow proxy.
         *
         * @param proxy Arrow proxy containing union array data and schema
         *
         * @pre proxy must contain valid Arrow union array and schema
         * @pre proxy format must be valid union format
         * @post Array is initialized with data from proxy
         * @post Type ID mapping is parsed and cached
         * @post Child arrays are created and accessible
         */
        explicit union_array_crtp_base(arrow_proxy proxy);

        /**
         * @brief Copy constructor.
         *
         * @param rhs Source union array to copy from
         *
         * @pre rhs must be in a valid state
         * @post This array contains a copy of rhs data
         * @post Child arrays and type mapping are reconstructed
         */
        constexpr union_array_crtp_base(const self_type& rhs);

        /**
         * @brief Copy assignment operator.
         *
         * @param rhs Source union array to copy from
         * @return Reference to this array
         *
         * @pre rhs must be in a valid state
         * @post This array contains a copy of rhs data
         * @post Previous data is properly released
         * @post Child arrays and type mapping are reconstructed
         */
        constexpr self_type& operator=(const self_type& rhs);

        constexpr union_array_crtp_base(self_type&& rhs) = default;
        constexpr self_type& operator=(self_type&& rhs) = default;

        /**
         * @brief Gets mutable reference to the Arrow proxy.
         *
         * @return Mutable reference to internal Arrow proxy
         *
         * @post Returns valid reference to Arrow proxy
         */
        [[nodiscard]] constexpr arrow_proxy& get_arrow_proxy();

        /**
         * @brief Gets const reference to the Arrow proxy.
         *
         * @return Const reference to internal Arrow proxy
         *
         * @post Returns valid const reference to Arrow proxy
         */
        [[nodiscard]] constexpr const arrow_proxy& get_arrow_proxy() const;

        arrow_proxy m_proxy;             ///< Internal Arrow proxy
        const std::uint8_t* p_type_ids;  ///< Pointer to type ID buffer
        children_type m_children;        ///< Child array wrappers
        type_id_map m_type_id_map;       ///< Type ID to child index mapping

        friend class detail::array_access;

#if defined(__cpp_lib_format)
        friend struct std::formatter<DERIVED>;
#endif
    };

    /**
     * @brief Equality comparison operator for union arrays.
     *
     * Compares two union arrays element-wise, ensuring both type IDs and values match.
     *
     * @tparam D Union array type
     * @param lhs First union array to compare
     * @param rhs Second union array to compare
     * @return true if arrays are element-wise equal, false otherwise
     *
     * @post Returns true iff arrays have same size and all elements compare equal
     * @post Comparison includes both type IDs and actual values
     */
    template <class D>
    constexpr bool operator==(const union_array_crtp_base<D>& lhs, const union_array_crtp_base<D>& rhs);

    /**
     * @brief Dense union array implementation with offset buffer.
     *
     * Dense union arrays store an additional offset buffer that maps each element
     * to its position within the corresponding child array. This allows child arrays
     * to be densely packed (only containing values that are actually used), making
     * them more memory efficient when union elements are sparse.
     *
     * Memory layout:
     * - Type ID buffer: Maps each element to child array type
     * - Offset buffer: Maps each element to position in child array
     * - Child arrays: Contain only the values actually used
     *
     * Related Apache Arrow specification:
     * https://arrow.apache.org/docs/dev/format/Columnar.html#dense-union
     *
     * @post Maintains Arrow dense union format compatibility ("+ud:")
     * @post Child arrays can be shorter than the union array length
     * @post Provides memory-efficient storage for sparse union data
     *
     * @code{.cpp}
     * // Create dense union with int and string children
     * std::vector<array> children = {int_array, string_array};
     * type_id_buffer_type type_ids = {0, 1, 0, 1};        // alternating types
     * offset_buffer_type offsets = {0, 0, 1, 1};          // positions in child arrays
     *
     * dense_union_array union_arr(std::move(children),
     *                              std::move(type_ids),
     *                              std::move(offsets));
     * @endcode
     */
    class dense_union_array : public union_array_crtp_base<dense_union_array>
    {
    public:

        using base_type = union_array_crtp_base<dense_union_array>;
        using const_iterator = typename base_type::const_iterator;
        using offset_buffer_type = u8_buffer<std::uint32_t>;
        using size_type = typename base_type::size_type;
        using typed_value = typename base_type::typed_value;
        using type_id_buffer_type = typename base_type::type_id_buffer_type;

        /**
         * @brief Generic constructor for creating dense union arrays.
         *
         * Creates a dense union array from various input combinations including
         * child arrays, type IDs, offsets, and optional type mapping.
         *
         * @tparam Args Parameter pack for constructor arguments
         * @param args Constructor arguments (children, type_ids, offsets, etc.)
         *
         * @pre Args must match one of the create_proxy() overload signatures
         * @pre Args must exclude copy and move constructor signatures
         * @pre Children, type IDs, and offsets must have consistent sizes
         * @post Array is created with the specified children and configuration
         */
        template <class... Args>
            requires(mpl::excludes_copy_and_move_ctor_v<dense_union_array, Args...>)
        explicit dense_union_array(Args&&... args)
            : dense_union_array(create_proxy(std::forward<Args>(args)...))
        {
        }

        /**
         * @brief Constructs dense union array from Arrow proxy.
         *
         * @param proxy Arrow proxy containing dense union array data and schema
         *
         * @pre proxy must contain valid Arrow dense union array and schema
         * @pre proxy format must be "+ud:..."
         * @pre proxy must have type ID buffer and offset buffer
         * @post Array is initialized with data from proxy
         * @post Offset buffer pointer is cached for efficient access
         */
        SPARROW_API explicit dense_union_array(arrow_proxy proxy);

        /**
         * @brief Copy constructor.
         *
         * @param rhs Source dense union array to copy from
         *
         * @pre rhs must be in a valid state
         * @post This array contains a copy of rhs data
         * @post Offset buffer pointer is properly set
         */
        SPARROW_API dense_union_array(const dense_union_array& rhs);

        /**
         * @brief Copy assignment operator.
         *
         * @param rhs Source dense union array to copy from
         * @return Reference to this array
         *
         * @pre rhs must be in a valid state
         * @post This array contains a copy of rhs data
         * @post Previous data is properly released
         * @post Offset buffer pointer is updated
         */
        SPARROW_API dense_union_array& operator=(const dense_union_array& rhs);

        dense_union_array(dense_union_array&& rhs) = default;
        dense_union_array& operator=(dense_union_array&& rhs) = default;

    private:

        static constexpr size_type OFFSETS_BUFFER_INDEX = 1;

        /**
         * @brief Creates proxy for dense union array from child arrays, type IDs, and offsets.
         *
         * @tparam TYPE_MAPPING Optional type mapping range (default: std::vector<std::uint8_t>)
         * @tparam METADATA_RANGE Optional metadata range (default: std::vector<metadata_pair>)
         * @param children Child arrays for the union
         * @param element_type Type ID buffer
         * @param offsets Offset buffer
         * @param type_mapping Optional mapping from child index to type ID
         * @param name Optional name for the array
         * @param metadata Optional metadata for the array
         * @return Arrow proxy representing the dense union array
         *
         * @pre Children, type IDs, and offsets must have consistent sizes
         * @post Returns valid Arrow proxy for the dense union array
         */
        template <
            std::ranges::input_range TYPE_MAPPING = std::vector<std::uint8_t>,
            input_metadata_container METADATA_RANGE = std::vector<metadata_pair>>
            requires(std::convertible_to<std::ranges::range_value_t<TYPE_MAPPING>, std::uint8_t>)
        [[nodiscard]] static auto create_proxy(
            std::vector<array>&& children,
            type_id_buffer_type&& element_type,
            offset_buffer_type&& offsets,
            std::optional<TYPE_MAPPING>&& type_mapping = std::nullopt,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<METADATA_RANGE> metadata = std::nullopt
        ) -> arrow_proxy;

        /**
         * @brief Creates proxy for dense union array from child arrays, type IDs, and offsets.
         *
         * @tparam TYPE_ID_BUFFER_RANGE Range for type ID buffer
         * @tparam OFFSET_BUFFER_RANGE Range for offset buffer
         * @tparam TYPE_MAPPING Optional type mapping range (default: std::vector<std::uint8_t>)
         * @tparam METADATA_RANGE Optional metadata range (default: std::vector<metadata_pair>)
         * @param children Child arrays for the union
         * @param element_type Type ID buffer
         * @param offsets Offset buffer
         * @param type_mapping Optional mapping from child index to type ID
         * @param name Optional name for the array
         * @param metadata Optional metadata for the array
         * @return Arrow proxy representing the dense union array
         *
         * @pre Children, type IDs, and offsets must have consistent sizes
         * @post Returns valid Arrow proxy for the dense union array
         */
        template <
            std::ranges::input_range TYPE_ID_BUFFER_RANGE,
            std::ranges::input_range OFFSET_BUFFER_RANGE,
            std::ranges::input_range TYPE_MAPPING = std::vector<std::uint8_t>,
            input_metadata_container METADATA_RANGE = std::vector<metadata_pair>>
            requires(std::convertible_to<std::ranges::range_value_t<TYPE_MAPPING>, std::uint8_t>)
        [[nodiscard]] static arrow_proxy create_proxy(
            std::vector<array>&& children,
            TYPE_ID_BUFFER_RANGE&& element_type,
            OFFSET_BUFFER_RANGE&& offsets,
            std::optional<TYPE_MAPPING>&& type_mapping = std::nullopt,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<METADATA_RANGE> metadata = std::nullopt
        )
        {
            SPARROW_ASSERT_TRUE(element_type.size() == offsets.size());
            type_id_buffer_type element_type_buffer{std::move(element_type)};
            offset_buffer_type offsets_buffer{std::move(offsets)};
            return dense_union_array::create_proxy(
                std::forward<std::vector<array>>(children),
                std::move(element_type_buffer),
                std::move(offsets_buffer),
                std::move(type_mapping),
                std::forward<std::optional<std::string_view>>(name),
                std::forward<std::optional<METADATA_RANGE>>(metadata)
            );
        }

        /**
         * @brief Implementation of create_proxy() for dense union arrays.
         *
         * @tparam METADATA_RANGE Optional metadata range (default: std::vector<metadata_pair>)
         * @param children Child arrays for the union
         * @param element_type Type ID buffer
         * @param offsets Offset buffer
         * @param format Arrow format string
         * @param tim Type ID to child index mapping
         * @param name Optional name for the array
         * @param metadata Optional metadata for the array
         * @return Arrow proxy representing the dense union array
         *
         * @pre Children, type IDs, and offsets must have consistent sizes
         * @post Returns valid Arrow proxy for the dense union array
         */
        template <input_metadata_container METADATA_RANGE = std::vector<metadata_pair>>
        [[nodiscard]] static arrow_proxy create_proxy_impl(
            std::vector<array>&& children,
            type_id_buffer_type&& element_type,
            offset_buffer_type&& offsets,
            std::string&& format,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<METADATA_RANGE> metadata = std::nullopt
        );

        /**
         * @brief Implementation of create_proxy() for dense union arrays.
         *
         * @tparam METADATA_RANGE Optional metadata range (default: std::vector<metadata_pair>)
         * @param children Child arrays for the union
         * @param element_type Type ID buffer
         * @param offsets Offset buffer
         * @param format Arrow format string
         * @param tim Type ID to child index mapping
         * @param name Optional name for the array
         * @param metadata Optional metadata for the array
         * @return Arrow proxy representing the dense union array
         *
         * @pre Children, type IDs, and offsets must have consistent sizes
         * @post Returns valid Arrow proxy for the dense union array
         */
        template <
            std::ranges::input_range TYPE_ID_BUFFER_RANGE,
            std::ranges::input_range OFFSET_BUFFER_RANGE,
            input_metadata_container METADATA_RANGE = std::vector<metadata_pair>>
        [[nodiscard]] static arrow_proxy create_proxy_impl(
            std::vector<array>&& children,
            TYPE_ID_BUFFER_RANGE&& element_type,
            OFFSET_BUFFER_RANGE&& offsets,
            std::string&& format,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<METADATA_RANGE> metadata = std::nullopt
        )
        {
            SPARROW_ASSERT_TRUE(std::ranges::distance(element_type) == std::ranges::distance(offsets));
            SPARROW_ASSERT_TRUE(std::ranges::distance(element_type) == children.size());
            type_id_buffer_type element_type_buffer{std::move(element_type)};
            offset_buffer_type offsets_buffer{std::move(offsets)};
            return dense_union_array::create_proxy_impl(
                std::forward<std::vector<array>>(children),
                std::move(element_type_buffer),
                std::move(offsets_buffer),
                std::forward<std::string>(format),
                std::forward<std::optional<std::string_view>>(name),
                std::forward<std::optional<METADATA_RANGE>>(metadata)
            );
        }

        /**
         * @brief Gets the offset for an element in its child array.
         *
         * @param i Index of the element in the union array
         * @return Offset of the element in its corresponding child array
         *
         * @pre i must be < size()
         * @post Returns valid offset into the appropriate child array
         * @post Used internally for element access in dense layout
         */
        SPARROW_API std::size_t element_offset(std::size_t i) const;

        void insert_value(const_iterator pos, const typed_value& value, size_type count);

        void erase_values(const_iterator pos, size_type count);

        void throw_if_offsets_are_not_canonical_for_mutation(const char* operation) const;

        [[nodiscard]] size_type child_offset_before(size_type end_index, std::uint8_t type_id) const;

        void recompute_offsets(size_type new_length);

        [[nodiscard]] const std::uint32_t* make_offsets() const;

        const std::uint32_t* p_offsets;  ///< Pointer to offset buffer
        friend class union_array_crtp_base<dense_union_array>;
    };

    /**
     * @brief Sparse union array implementation without offset buffer.
     *
     * Sparse union arrays do not store an offset buffer. Instead, all child arrays
     * have the same length as the union array, and each element directly corresponds
     * to the same position in its child array. This is simpler but less memory
     * efficient when union elements are sparse.
     *
     * Memory layout:
     * - Type ID buffer: Maps each element to child array type
     * - Child arrays: All have the same length as the union array
     *
     * Related Apache Arrow specification:
     * https://arrow.apache.org/docs/dev/format/Columnar.html#sparse-union
     *
     * @post Maintains Arrow sparse union format compatibility ("+us:")
     * @post All child arrays have the same length as the union array
     * @post Provides simpler access pattern at the cost of memory efficiency
     *
     * @code{.cpp}
     * // Create sparse union with int and string children
     * std::vector<array> children = {int_array, string_array};  // same length as union
     * type_id_buffer_type type_ids = {0, 1, 0, 1};              // alternating types
     *
     * sparse_union_array union_arr(std::move(children),
     *                               std::move(type_ids));
     * @endcode
     */
    class sparse_union_array : public union_array_crtp_base<sparse_union_array>
    {
    public:

        using base_type = union_array_crtp_base<sparse_union_array>;
        using const_iterator = typename base_type::const_iterator;
        using size_type = typename base_type::size_type;
        using typed_value = typename base_type::typed_value;
        using type_id_buffer_type = typename base_type::type_id_buffer_type;

        /**
         * @brief Generic constructor for creating sparse union arrays.
         *
         * Creates a sparse union array from various input combinations including
         * child arrays, type IDs, and optional type mapping.
         *
         * @tparam Args Parameter pack for constructor arguments
         * @param args Constructor arguments (children, type_ids, etc.)
         *
         * @pre Args must match one of the create_proxy() overload signatures
         * @pre Args must exclude copy and move constructor signatures
         * @pre All child arrays must have the same length
         * @post Array is created with the specified children and configuration
         */
        template <class... Args>
            requires(mpl::excludes_copy_and_move_ctor_v<sparse_union_array, Args...>)
        explicit sparse_union_array(Args&&... args)
            : sparse_union_array(create_proxy(std::forward<Args>(args)...))
        {
        }

        /**
         * @brief Constructs sparse union array from Arrow proxy.
         *
         * @param proxy Arrow proxy containing sparse union array data and schema
         *
         * @pre proxy must contain valid Arrow sparse union array and schema
         * @pre proxy format must be "+us:..."
         * @pre proxy must have type ID buffer
         * @pre All child arrays must have same length as union array
         * @post Array is initialized with data from proxy
         */
        SPARROW_API explicit sparse_union_array(arrow_proxy proxy);

        SPARROW_API sparse_union_array(const sparse_union_array&);
        SPARROW_API sparse_union_array& operator=(const sparse_union_array&);

    private:

        /**
         * @brief Creates proxy for sparse union array from child arrays, type IDs, and optional type mapping.
         *
         * @tparam TYPE_MAPPING Optional type mapping range (default: std::vector<std::uint8_t>)
         * @tparam METADATA_RANGE Optional metadata range (default: std::vector<metadata_pair>)
         * @param children Child arrays for the union
         * @param element_type Type ID buffer
         * @param type_mapping Optional mapping from child index to type ID
         * @param name Optional name for the array
         * @param metadata Optional metadata for the array
         * @return Arrow proxy representing the sparse union array
         *
         * @pre All child arrays must have the same length
         * @post Returns valid Arrow proxy for the sparse union array
         */
        template <
            std::ranges::input_range TYPE_MAPPING = std::vector<std::uint8_t>,
            input_metadata_container METADATA_RANGE = std::vector<metadata_pair>>
            requires(std::convertible_to<std::ranges::range_value_t<TYPE_MAPPING>, std::uint8_t>)
        static auto create_proxy(
            std::vector<array>&& children,
            type_id_buffer_type&& element_type,
            std::optional<TYPE_MAPPING>&& type_mapping = std::nullopt,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<METADATA_RANGE> metadata = std::nullopt
        ) -> arrow_proxy;

        /**
         * @brief Implementation of create_proxy() for sparse union arrays.
         *
         * @tparam METADATA_RANGE Optional metadata range (default: std::vector<metadata_pair>)
         * @param children Child arrays for the union
         * @param element_type Type ID buffer
         * @param format Arrow format string
         * @param tim Type ID to child index mapping
         * @param name Optional name for the array
         * @param metadata Optional metadata for the array
         * @return Arrow proxy representing the sparse union array
         *
         * @pre All child arrays must have the same length
         * @post Returns valid Arrow proxy for the sparse union array
         */
        template <input_metadata_container METADATA_RANGE>
        static auto create_proxy_impl(
            std::vector<array>&& children,
            type_id_buffer_type&& element_type,
            std::string&& format,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<METADATA_RANGE> metadata = std::nullopt
        ) -> arrow_proxy;

        /**
         * @brief Gets the offset for an element in its child array.
         *
         * For sparse unions, this always returns the same index as the union array
         * since all child arrays have the same length.
         *
         * @param i Index of the element in the union array
         * @return The same index i (direct correspondence)
         *
         * @pre i must be < size()
         * @post Returns i (sparse layout uses direct indexing)
         * @post Used internally for element access in sparse layout
         */
        [[nodiscard]] SPARROW_API std::size_t element_offset(std::size_t i) const;

        void insert_value(const_iterator pos, const typed_value& value, size_type count);

        void erase_values(const_iterator pos, size_type count);

        friend class union_array_crtp_base<sparse_union_array>;
    };

    /****************************************
     * union_array_crtp_base implementation *
     ****************************************/

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::parse_type_id_map(std::string_view format_string)
        -> type_id_map
    {
        type_id_map ret;
        ret.fill(INVALID_CHILD_INDEX);
        // remove +du: / +su: prefix
        format_string.remove_prefix(4);

        constexpr std::string_view delim{","};
        std::size_t child_index = 0;
        std::ranges::for_each(
            format_string | std::views::split(delim),
            [&](const auto& s)
            {
                const std::string str(
                    std::string_view{&*std::ranges::begin(s), static_cast<size_t>(std::ranges::distance(s))}
                );
                const auto as_int = std::atoi(str.c_str());
                ret[static_cast<std::size_t>(as_int)] = child_index;
                ++child_index;
            }
        );
        return ret;
    }

    template <class DERIVED>
    template <std::ranges::input_range R>
    constexpr auto
    union_array_crtp_base<DERIVED>::type_id_map_from_child_to_type_id(const std::optional<R>& child_index_to_type_id)
        -> type_id_map
    {
        type_id_map ret;
        ret.fill(INVALID_CHILD_INDEX);
        if (!child_index_to_type_id.has_value())
        {
            constexpr type_id_map default_mapping = []
            {
                type_id_map arr{};
                std::iota(arr.begin(), arr.end(), 0);
                return arr;
            }();
            return default_mapping;
        }
        else
        {
            const std::size_t n = std::ranges::size(*child_index_to_type_id);
            for (std::size_t i = 0; i < n; ++i)
            {
                ret[(*child_index_to_type_id)[static_cast<std::uint8_t>(i)]] = i;
            }
        }
        return ret;
    }

    template <class DERIVED>
    template <std::ranges::input_range R>
        requires(std::convertible_to<std::ranges::range_value_t<R>, std::uint8_t>)
    constexpr std::string
    union_array_crtp_base<DERIVED>::make_format_string(bool dense, const std::size_t n, const std::optional<R>& range)
    {
        const auto range_size = range.has_value() ? std::ranges::size(*range) : 0;
        if (range_size == n || range_size == 0)
        {
            std::string ret = dense ? "+ud:" : "+us:";
            if (range_size == 0)
            {
                for (std::size_t i = 0; i < n; ++i)
                {
                    ret += std::to_string(i) + ",";
                }
            }
            else
            {
                for (const auto& v : *range)
                {
                    ret += std::to_string(v) + ",";
                }
            }
            ret.pop_back();
            return ret;
        }
        else
        {
            throw std::invalid_argument("Invalid type-id map");
        }
    }

    template <class DERIVED>
    constexpr std::optional<std::string_view> union_array_crtp_base<DERIVED>::name() const
    {
        return m_proxy.name();
    }

    template <class DERIVED>
    SPARROW_CONSTEXPR_CLANG std::optional<key_value_view> union_array_crtp_base<DERIVED>::metadata() const
    {
        return m_proxy.metadata();
    }

    template <class DERIVED>
    SPARROW_CONSTEXPR_CLANG auto union_array_crtp_base<DERIVED>::at(size_type i) const -> value_type
    {
        if (i >= size())
        {
            throw std::out_of_range("Union index out of range");
        }
        return (*this)[i];
    }

    template <class DERIVED>
    constexpr arrow_proxy& union_array_crtp_base<DERIVED>::get_arrow_proxy()
    {
        return m_proxy;
    }

    template <class DERIVED>
    constexpr const arrow_proxy& union_array_crtp_base<DERIVED>::get_arrow_proxy() const
    {
        return m_proxy;
    }

    template <class DERIVED>
    union_array_crtp_base<DERIVED>::union_array_crtp_base(arrow_proxy proxy)
        : m_proxy(std::move(proxy))
        , p_type_ids(make_type_ids())
        , m_children(make_children(m_proxy))
        , m_type_id_map(parse_type_id_map(m_proxy.format()))
    {
    }

    template <class DERIVED>
    constexpr union_array_crtp_base<DERIVED>::union_array_crtp_base(const self_type& rhs)
        : self_type(rhs.m_proxy)
    {
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::operator=(const self_type& rhs) -> self_type&
    {
        if (this != &rhs)
        {
            m_proxy = rhs.m_proxy;
            refresh_type_id_cache();
            m_children = make_children(m_proxy);
            m_type_id_map = parse_type_id_map(m_proxy.format());
        }
        return *this;
    }

    template <class DERIVED>
    SPARROW_CONSTEXPR_CLANG auto union_array_crtp_base<DERIVED>::operator[](std::size_t i) const -> value_type
    {
        const auto type_id = static_cast<std::size_t>(p_type_ids[i]);
        const auto child_index = m_type_id_map[type_id];
        const auto offset = this->derived_cast().element_offset(i);
        return m_children[child_index][static_cast<std::size_t>(offset)];
    }

    template <class DERIVED>
    SPARROW_CONSTEXPR_CLANG auto union_array_crtp_base<DERIVED>::operator[](std::size_t i) -> value_type
    {
        return static_cast<const derived_type&>(*this)[i];
    }

    template <class DERIVED>
    constexpr std::size_t union_array_crtp_base<DERIVED>::size() const
    {
        return m_proxy.length();
    }

    template <class DERIVED>
    constexpr bool union_array_crtp_base<DERIVED>::empty() const
    {
        return size() == 0;
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::insert(const_iterator pos, const typed_value& value) -> iterator
    {
        return insert(pos, value, 1);
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::insert(const_iterator pos, const typed_value& value, size_type count)
        -> iterator
    {
        SPARROW_ASSERT_TRUE(pos >= cbegin());
        SPARROW_ASSERT_TRUE(pos <= cend());

        const size_type idx = static_cast<size_type>(std::distance(cbegin(), pos));
        if (count == 0)
        {
            return sparrow::next(begin(), static_cast<std::ptrdiff_t>(idx));
        }

        this->derived_cast().insert_value(pos, value, count);
        get_arrow_proxy().set_length(size() + count);
        return sparrow::next(begin(), static_cast<std::ptrdiff_t>(idx));
    }

    template <class DERIVED>
    template <std::input_iterator InputIt>
        requires std::convertible_to<
            typename std::iterator_traits<InputIt>::value_type,
            typename union_array_crtp_base<DERIVED>::typed_value>
    auto union_array_crtp_base<DERIVED>::insert(const_iterator pos, InputIt first, InputIt last) -> iterator
    {
        SPARROW_ASSERT_TRUE(pos >= cbegin());
        SPARROW_ASSERT_TRUE(pos <= cend());

        const size_type idx = static_cast<size_type>(std::distance(cbegin(), pos));
        auto& proxy = get_arrow_proxy();
        const size_type original_size = size();
        size_type inserted_count = 0;

        for (auto it = first; it != last; ++it)
        {
            const auto current_pos = sparrow::next(cbegin(), static_cast<std::ptrdiff_t>(idx + inserted_count));
            this->derived_cast().insert_value(current_pos, static_cast<typed_value>(*it), 1);
            ++inserted_count;
            proxy.set_length(original_size + inserted_count);
        }

        return sparrow::next(begin(), static_cast<std::ptrdiff_t>(idx));
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::erase(const_iterator pos) -> iterator
    {
        return erase(pos, sparrow::next(pos, 1));
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::erase(const_iterator first, const_iterator last) -> iterator
    {
        SPARROW_ASSERT_TRUE(first >= cbegin());
        SPARROW_ASSERT_TRUE(first <= cend());
        SPARROW_ASSERT_TRUE(last >= first);
        SPARROW_ASSERT_TRUE(last <= cend());

        const size_type idx = static_cast<size_type>(std::distance(cbegin(), first));
        const size_type count = static_cast<size_type>(std::distance(first, last));
        if (count == 0)
        {
            return sparrow::next(begin(), static_cast<std::ptrdiff_t>(idx));
        }

        this->derived_cast().erase_values(first, count);
        get_arrow_proxy().set_length(size() - count);
        return sparrow::next(begin(), static_cast<std::ptrdiff_t>(idx));
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::push_back(const typed_value& value)
    {
        static_cast<void>(insert(cend(), value));
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::pop_back()
    {
        SPARROW_ASSERT_TRUE(!empty());
        static_cast<void>(erase(sparrow::next(cbegin(), static_cast<std::ptrdiff_t>(size() - 1))));
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::resize(size_type new_length, const typed_value& value)
    {
        const size_type current_size = size();
        if (new_length < current_size)
        {
            static_cast<void>(erase(sparrow::next(cbegin(), static_cast<std::ptrdiff_t>(new_length)), cend()));
        }
        else if (new_length > current_size)
        {
            static_cast<void>(insert(cend(), value, new_length - current_size));
        }
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::clear()
    {
        if (!empty())
        {
            static_cast<void>(erase(cbegin(), cend()));
        }
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::begin() -> iterator
    {
        return iterator(functor_type{&(this->derived_cast())}, 0);
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::end() -> iterator
    {
        return iterator(functor_type{&(this->derived_cast())}, this->size());
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::begin() const -> const_iterator
    {
        return cbegin();
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::end() const -> const_iterator
    {
        return cend();
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::cbegin() const -> const_iterator
    {
        return const_iterator(const_functor_type{&(this->derived_cast())}, 0);
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::cend() const -> const_iterator
    {
        return const_iterator(const_functor_type{&(this->derived_cast())}, this->size());
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::rbegin() const -> const_reverse_iterator
    {
        return const_reverse_iterator{cend()};
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::rend() const -> const_reverse_iterator
    {
        return const_reverse_iterator{cbegin()};
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::crbegin() const -> const_reverse_iterator
    {
        return rbegin();
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::crend() const -> const_reverse_iterator
    {
        return rend();
    }

    template <class DERIVED>
    SPARROW_CONSTEXPR_CLANG auto union_array_crtp_base<DERIVED>::front() const -> value_type
    {
        return (*this)[0];
    }

    template <class DERIVED>
    SPARROW_CONSTEXPR_CLANG auto union_array_crtp_base<DERIVED>::back() const -> value_type
    {
        return (*this)[this->size() - 1];
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::throw_if_sliced_for_mutation(const char* operation) const
    {
        if (get_arrow_proxy().offset() != 0)
        {
            throw std::logic_error(std::string(operation) + " does not support sliced arrays (non-zero offset)");
        }
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::child_index_from_type_id(std::uint8_t type_id) const -> size_type
    {
        const size_type child_index = m_type_id_map[static_cast<size_type>(type_id)];
        if (child_index == INVALID_CHILD_INDEX || child_index >= m_children.size())
        {
            throw std::out_of_range("Unknown union type id");
        }
        return child_index;
    }

    template <class DERIVED>
    bool union_array_crtp_base<DERIVED>::child_is_nullable(size_type child_index) const
    {
        return m_proxy.children()[child_index].flags().contains(ArrowFlag::NULLABLE);
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::make_type_ids() const -> const std::uint8_t*
    {
        return m_proxy.buffers()[TYPE_IDS_BUFFER_INDEX].template data<std::uint8_t>()
               + static_cast<size_type>(m_proxy.offset());
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::refresh_type_id_cache()
    {
        p_type_ids = make_type_ids();
    }

    template <class DERIVED>
    array& union_array_crtp_base<DERIVED>::raw_child_wrapper(size_type child_index)
    {
        return m_children[child_index];
    }

    template <class DERIVED>
    const array& union_array_crtp_base<DERIVED>::child_wrapper(size_type child_index) const
    {
        return m_children[child_index];
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::validate_child_insert_value(
        const array& child,
        const array_traits::value_type& value
    ) const
    {
        detail::validate_union_child_insert_value(child, value);
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::validate_child_erase(const array& child) const
    {
        detail::validate_union_child_erase(child);
    }

    template <class DERIVED>
    void union_array_crtp_base<DERIVED>::insert_child_value(
        array& child,
        size_type pos,
        const array_traits::value_type& value,
        size_type count
    ) const
    {
        detail::insert_union_child_value(child, pos, value, count);
    }

    template <class DERIVED>
    auto union_array_crtp_base<DERIVED>::make_inactive_child_value(size_type child_index) const
        -> array_traits::value_type
    {
        return detail::make_union_child_default_value(child_wrapper(child_index), !child_is_nullable(child_index));
    }

    template <class DERIVED>
    constexpr auto union_array_crtp_base<DERIVED>::make_children(arrow_proxy& proxy) -> children_type
    {
        children_type children;
        children.reserve(proxy.children().size());
        for (std::size_t i = 0; i < proxy.children().size(); ++i)
        {
            auto& child_proxy = proxy.children()[i];
            children.emplace_back(&child_proxy.array(), &child_proxy.schema());
        }
        return children;
    }

    inline auto dense_union_array::make_offsets() const -> const std::uint32_t*
    {
        return this->get_arrow_proxy().buffers()[OFFSETS_BUFFER_INDEX].template data<std::uint32_t>()
               + static_cast<size_type>(this->get_arrow_proxy().offset());
    }

    inline auto dense_union_array::child_offset_before(size_type end_index, std::uint8_t type_id) const
        -> size_type
    {
        size_type offset = 0;
        for (size_type i = 0; i < end_index; ++i)
        {
            if (this->p_type_ids[i] == type_id)
            {
                ++offset;
            }
        }
        return offset;
    }

    inline void dense_union_array::throw_if_offsets_are_not_canonical_for_mutation(const char* operation) const
    {
        std::vector<size_type> child_offsets(this->m_children.size(), 0);

        for (size_type i = 0; i < this->size(); ++i)
        {
            const auto child_index = this->child_index_from_type_id(this->p_type_ids[i]);
            if (static_cast<size_type>(p_offsets[i]) != child_offsets[child_index])
            {
                throw std::logic_error(std::string(operation) + " requires canonical dense-union offsets");
            }
            ++child_offsets[child_index];
        }

        for (size_type child_index = 0; child_index < child_offsets.size(); ++child_index)
        {
            if (child_offsets[child_index] != this->child_wrapper(child_index).size())
            {
                throw std::logic_error(std::string(operation) + " requires densely packed child arrays");
            }
        }
    }

    inline void dense_union_array::recompute_offsets(size_type new_length)
    {
        auto& proxy = this->get_arrow_proxy();
        auto& type_id_buffer = proxy.get_array_private_data()->buffers()[TYPE_IDS_BUFFER_INDEX];
        auto type_id_adaptor = make_buffer_adaptor<std::uint8_t>(type_id_buffer);

        auto& offset_buffer = proxy.get_array_private_data()->buffers()[OFFSETS_BUFFER_INDEX];
        auto offset_adaptor = make_buffer_adaptor<std::uint32_t>(offset_buffer);
        offset_adaptor.resize(new_length);

        std::vector<size_type> child_offsets(this->m_children.size(), 0);
        constexpr size_type max_offset = static_cast<size_type>(std::numeric_limits<std::uint32_t>::max());

        for (size_type i = 0; i < new_length; ++i)
        {
            const auto child_index = this->child_index_from_type_id(type_id_adaptor[i]);
            SPARROW_ASSERT_TRUE(child_offsets[child_index] <= max_offset);
            offset_adaptor[i] = static_cast<std::uint32_t>(child_offsets[child_index]++);
        }

        for (size_type child_index = 0; child_index < child_offsets.size(); ++child_index)
        {
            SPARROW_ASSERT_TRUE(child_offsets[child_index] == this->child_wrapper(child_index).size());
        }
    }

    inline void dense_union_array::insert_value(const_iterator pos, const typed_value& value, size_type count)
    {
        if (count == 0)
        {
            return;
        }

        const size_type idx = static_cast<size_type>(std::distance(this->cbegin(), pos));
        this->throw_if_sliced_for_mutation("dense_union_array::insert_value");
        throw_if_offsets_are_not_canonical_for_mutation("dense_union_array::insert_value");

        const auto child_index = this->child_index_from_type_id(value.type_id);
        this->validate_child_insert_value(this->child_wrapper(child_index), value.value);

        const size_type child_insert_pos = child_offset_before(idx, value.type_id);
        this->insert_child_value(this->raw_child_wrapper(child_index), child_insert_pos, value.value, count);

        auto& proxy = this->get_arrow_proxy();
        auto& type_id_buffer = proxy.get_array_private_data()->buffers()[TYPE_IDS_BUFFER_INDEX];
        auto type_id_adaptor = make_buffer_adaptor<std::uint8_t>(type_id_buffer);
        type_id_adaptor.insert(
            sparrow::next(type_id_adaptor.cbegin(), static_cast<std::ptrdiff_t>(idx)),
            count,
            value.type_id
        );

        recompute_offsets(type_id_adaptor.size());
        proxy.update_buffers();
        this->refresh_type_id_cache();
        p_offsets = make_offsets();
    }

    inline void dense_union_array::erase_values(const_iterator pos, size_type count)
    {
        if (count == 0)
        {
            return;
        }

        const size_type idx = static_cast<size_type>(std::distance(this->cbegin(), pos));
        this->throw_if_sliced_for_mutation("dense_union_array::erase_values");
        throw_if_offsets_are_not_canonical_for_mutation("dense_union_array::erase_values");

        std::vector<size_type> prefix_counts(this->m_children.size(), 0);
        for (size_type i = 0; i < idx; ++i)
        {
            ++prefix_counts[this->child_index_from_type_id(this->p_type_ids[i])];
        }

        std::vector<size_type> erase_counts(this->m_children.size(), 0);
        for (size_type i = idx; i < idx + count; ++i)
        {
            ++erase_counts[this->child_index_from_type_id(this->p_type_ids[i])];
        }

        for (size_type child_index = 0; child_index < erase_counts.size(); ++child_index)
        {
            if (erase_counts[child_index] != 0)
            {
                this->validate_child_erase(this->child_wrapper(child_index));
            }
        }

        for (size_type child_index = 0; child_index < erase_counts.size(); ++child_index)
        {
            if (erase_counts[child_index] != 0)
            {
                auto& child = this->raw_child_wrapper(child_index);
                child.erase(
                    child.cbegin() + static_cast<std::ptrdiff_t>(prefix_counts[child_index]),
                    child.cbegin()
                        + static_cast<std::ptrdiff_t>(prefix_counts[child_index] + erase_counts[child_index])
                );
            }
        }

        auto& proxy = this->get_arrow_proxy();
        auto& type_id_buffer = proxy.get_array_private_data()->buffers()[TYPE_IDS_BUFFER_INDEX];
        auto type_id_adaptor = make_buffer_adaptor<std::uint8_t>(type_id_buffer);
        type_id_adaptor.erase(
            sparrow::next(type_id_adaptor.cbegin(), static_cast<std::ptrdiff_t>(idx)),
            sparrow::next(type_id_adaptor.cbegin(), static_cast<std::ptrdiff_t>(idx + count))
        );

        recompute_offsets(type_id_adaptor.size());
        proxy.update_buffers();
        this->refresh_type_id_cache();
        p_offsets = make_offsets();
    }

    inline void sparse_union_array::insert_value(const_iterator pos, const typed_value& value, size_type count)
    {
        if (count == 0)
        {
            return;
        }

        const size_type idx = static_cast<size_type>(std::distance(this->cbegin(), pos));
        this->throw_if_sliced_for_mutation("sparse_union_array::insert_value");

        const auto active_child_index = this->child_index_from_type_id(value.type_id);
        std::vector<array_traits::value_type> child_values;
        child_values.reserve(this->m_children.size());

        for (size_type child_index = 0; child_index < this->m_children.size(); ++child_index)
        {
            child_values.push_back(
                child_index == active_child_index ? value.value : this->make_inactive_child_value(child_index)
            );
            this->validate_child_insert_value(this->child_wrapper(child_index), child_values.back());
        }

        for (size_type child_index = 0; child_index < this->m_children.size(); ++child_index)
        {
            this->insert_child_value(this->raw_child_wrapper(child_index), idx, child_values[child_index], count);
        }

        auto& proxy = this->get_arrow_proxy();
        auto& type_id_buffer = proxy.get_array_private_data()->buffers()[TYPE_IDS_BUFFER_INDEX];
        auto type_id_adaptor = make_buffer_adaptor<std::uint8_t>(type_id_buffer);
        type_id_adaptor.insert(
            sparrow::next(type_id_adaptor.cbegin(), static_cast<std::ptrdiff_t>(idx)),
            count,
            value.type_id
        );

        proxy.update_buffers();
        this->refresh_type_id_cache();
    }

    inline void sparse_union_array::erase_values(const_iterator pos, size_type count)
    {
        if (count == 0)
        {
            return;
        }

        const size_type idx = static_cast<size_type>(std::distance(this->cbegin(), pos));
        this->throw_if_sliced_for_mutation("sparse_union_array::erase_values");

        for (size_type child_index = 0; child_index < this->m_children.size(); ++child_index)
        {
            this->validate_child_erase(this->child_wrapper(child_index));
        }

        for (size_type child_index = 0; child_index < this->m_children.size(); ++child_index)
        {
            auto& child = this->raw_child_wrapper(child_index);
            child.erase(
                child.cbegin() + static_cast<std::ptrdiff_t>(idx),
                child.cbegin() + static_cast<std::ptrdiff_t>(idx + count)
            );
        }

        auto& proxy = this->get_arrow_proxy();
        auto& type_id_buffer = proxy.get_array_private_data()->buffers()[TYPE_IDS_BUFFER_INDEX];
        auto type_id_adaptor = make_buffer_adaptor<std::uint8_t>(type_id_buffer);
        type_id_adaptor.erase(
            sparrow::next(type_id_adaptor.cbegin(), static_cast<std::ptrdiff_t>(idx)),
            sparrow::next(type_id_adaptor.cbegin(), static_cast<std::ptrdiff_t>(idx + count))
        );

        proxy.update_buffers();
        this->refresh_type_id_cache();
    }

    template <class D>
    constexpr bool operator==(const union_array_crtp_base<D>& lhs, const union_array_crtp_base<D>& rhs)
    {
        return std::ranges::equal(lhs, rhs);
    }

    /************************************
     * Union array shared implementation *
     ************************************/

    namespace detail
    {
        template <input_metadata_container METADATA_RANGE = std::vector<metadata_pair>>
        arrow_proxy create_union_proxy_impl(
            std::vector<array>&& children,
            std::vector<buffer<std::uint8_t>>&& buffers,
            std::size_t size,
            std::string&& format,
            std::optional<std::string_view> name,
            std::optional<METADATA_RANGE> metadata
        )
        {
            const auto n_children = children.size();
            ArrowSchema** child_schemas = new ArrowSchema*[n_children];
            ArrowArray** child_arrays = new ArrowArray*[n_children];

            for (std::size_t i = 0; i < n_children; ++i)
            {
                auto& child = children[i];
                auto [flat_arr, flat_schema] = extract_arrow_structures(std::move(child));
                child_arrays[i] = new ArrowArray(std::move(flat_arr));
                child_schemas[i] = new ArrowSchema(std::move(flat_schema));
            }

            const bool is_nullable = std::all_of(
                child_schemas,
                child_schemas + n_children,
                [](const ArrowSchema* schema)
                {
                    return to_set_of_ArrowFlags(schema->flags).contains(ArrowFlag::NULLABLE);
                }
            );

            const std::optional<std::unordered_set<sparrow::ArrowFlag>>
                flags = is_nullable
                            ? std::make_optional(std::unordered_set<sparrow::ArrowFlag>{ArrowFlag::NULLABLE})
                            : std::nullopt;

            ArrowSchema schema = make_arrow_schema(
                std::move(format),
                std::move(name),                      // name
                std::move(metadata),                  // metadata
                flags,                                // flags,
                child_schemas,                        // children
                repeat_view<bool>(true, n_children),  // children_ownership
                nullptr,                              // dictionary,
                true                                  // dictionary ownership
            );

            ArrowArray arr = make_arrow_array(
                static_cast<std::int64_t>(size),  // length
                0,                                // null_count: always 0 as the nullability is in children
                0,                                // offset
                std::move(buffers),
                child_arrays,                         // children
                repeat_view<bool>(true, n_children),  // children_ownership
                nullptr,                              // dictionary
                true
            );

            return arrow_proxy{std::move(arr), std::move(schema)};
        }
    }

    /************************************
     * dense_union_array implementation *
     ************************************/

    template <std::ranges::input_range TYPE_MAPPING, input_metadata_container METADATA_RANGE>
        requires(std::convertible_to<std::ranges::range_value_t<TYPE_MAPPING>, std::uint8_t>)
    auto dense_union_array::create_proxy(
        std::vector<array>&& children,
        type_id_buffer_type&& element_type,
        offset_buffer_type&& offsets,
        std::optional<TYPE_MAPPING>&& child_index_to_type_id,
        std::optional<std::string_view> name,
        std::optional<METADATA_RANGE> metadata
    ) -> arrow_proxy
    {
        SPARROW_ASSERT_TRUE(element_type.size() == offsets.size());
        const auto n_children = children.size();

        std::string format = make_format_string(true /*dense union*/, n_children, child_index_to_type_id);

        return create_proxy_impl(
            std::move(children),
            std::move(element_type),
            std::move(offsets),
            std::move(format),
            std::move(name),
            std::move(metadata)
        );
    }

    template <input_metadata_container METADATA_RANGE>
    auto dense_union_array::create_proxy_impl(
        std::vector<array>&& children,
        type_id_buffer_type&& element_type,
        offset_buffer_type&& offsets,
        std::string&& format,
        std::optional<std::string_view> name,
        std::optional<METADATA_RANGE> metadata
    ) -> arrow_proxy
    {
        SPARROW_ASSERT_TRUE(element_type.size() == offsets.size());
        const auto size = element_type.size();

        std::vector<buffer<std::uint8_t>> arr_buffs;
        arr_buffs.reserve(2);
        arr_buffs.emplace_back(std::move(element_type).extract_storage());
        arr_buffs.emplace_back(std::move(offsets).extract_storage());

        return detail::create_union_proxy_impl(
            std::move(children),
            std::move(arr_buffs),
            size,
            std::move(format),
            std::move(name),
            std::move(metadata)
        );
    }

    /*************************************
     * sparse_union_array implementation *
     *************************************/

    template <std::ranges::input_range TYPE_MAPPING, input_metadata_container METADATA_RANGE>
        requires(std::convertible_to<std::ranges::range_value_t<TYPE_MAPPING>, std::uint8_t>)
    auto sparse_union_array::create_proxy(
        std::vector<array>&& children,
        type_id_buffer_type&& element_type,
        std::optional<TYPE_MAPPING>&& child_index_to_type_id,
        std::optional<std::string_view> name,
        std::optional<METADATA_RANGE> metadata
    ) -> arrow_proxy
    {
        const auto n_children = children.size();
        if (child_index_to_type_id.has_value())
        {
            SPARROW_ASSERT_TRUE((*child_index_to_type_id).size() == n_children);
        }

        std::string format = make_format_string(false /*is dense union*/, n_children, child_index_to_type_id);

        return create_proxy_impl(
            std::move(children),
            std::move(element_type),
            std::move(format),
            std::move(name),
            std::move(metadata)
        );
    }

    template <input_metadata_container METADATA_RANGE>
    auto sparse_union_array::create_proxy_impl(
        std::vector<array>&& children,
        type_id_buffer_type&& element_type,
        std::string&& format,
        std::optional<std::string_view> name,
        std::optional<METADATA_RANGE> metadata
    ) -> arrow_proxy
    {
        for (const auto& child : children)
        {
            SPARROW_ASSERT_TRUE(child.size() == element_type.size());
        }
        const auto size = element_type.size();

        std::vector<buffer<std::uint8_t>> arr_buffs;
        arr_buffs.reserve(1);
        arr_buffs.emplace_back(std::move(element_type).extract_storage());

        return detail::create_union_proxy_impl(
            std::move(children),
            std::move(arr_buffs),
            size,
            std::move(format),
            std::move(name),
            std::move(metadata)
        );
    }
}

#if defined(__cpp_lib_format)

/**
 * @brief std::format formatter specialization for union arrays.
 *
 * Provides formatting support for union arrays in std::format contexts.
 * Outputs the union type, metadata, and element values.
 *
 * @tparam U Union array type (dense_union_array or sparse_union_array)
 */
template <typename U>
    requires std::derived_from<U, sparrow::union_array_crtp_base<U>>
struct std::formatter<U>
{
    /**
     * @brief Parses format specification (currently unused).
     *
     * @param ctx Format parse context
     * @return Iterator to end of format specification
     */
    constexpr auto parse(std::format_parse_context& ctx)
    {
        return ctx.begin();  // Simple implementation
    }

    /**
     * @brief Formats the union array for output.
     *
     * @param ar Union array to format
     * @param ctx Format context
     * @return Iterator to end of formatted output
     *
     * @post Outputs format: "DenseUnion|SparseUnion [name=NAME | size=SIZE] <elem1, elem2, ..., elemN>"
     * @post Name shows "nullptr" if not set
     */
    auto format(const U& ar, std::format_context& ctx) const
    {
        if constexpr (std::is_same_v<U, sparrow::dense_union_array>)
        {
            std::format_to(ctx.out(), "DenseUnion");
        }
        else if constexpr (std::is_same_v<U, sparrow::sparse_union_array>)
        {
            std::format_to(ctx.out(), "SparseUnion");
        }
        else
        {
            static_assert(sparrow::mpl::dependent_false<U>::value, "Unknown union array type");
            sparrow::mpl::unreachable();
        }
        const auto& proxy = ar.get_arrow_proxy();
        std::format_to(ctx.out(), " [name={} | size={}] <", proxy.name().value_or("nullptr"), proxy.length());

        std::for_each(
            ar.cbegin(),
            std::prev(ar.cend()),
            [&ctx](const auto& value)
            {
                std::format_to(ctx.out(), "{}, ", value);
            }
        );

        return std::format_to(ctx.out(), "{}>", ar.back());
    }
};

namespace sparrow
{
    /**
     * @brief Stream output operator for union arrays.
     *
     * @tparam U Union array type (dense_union_array or sparse_union_array)
     * @param os Output stream
     * @param value Union array to output
     * @return Reference to the output stream
     *
     * @post Outputs the union array using std::format formatter
     */
    template <typename U>
        requires std::derived_from<U, union_array_crtp_base<U>>
    std::ostream& operator<<(std::ostream& os, const U& value)
    {
        os << std::format("{}", value);
        return os;
    }
}

#endif
