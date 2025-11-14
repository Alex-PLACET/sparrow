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

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "sparrow/array_api.hpp"
#include "sparrow/buffer/dynamic_bitset/dynamic_bitset.hpp"
#include "sparrow/layout/array_bitmap_base.hpp"
#include "sparrow/layout/array_wrapper.hpp"
#include "sparrow/layout/layout_utils.hpp"
#include "sparrow/list_array.hpp"
#include "sparrow/utils/functor_index_iterator.hpp"
#include "sparrow/utils/memory.hpp"
#include "sparrow/utils/mp_utils.hpp"
#include "sparrow/utils/nullable.hpp"

namespace sparrow
{
    class fixed_shape_tensor_array;

    template <class T>
    constexpr bool is_fixed_shape_tensor_array_v = std::same_as<T, fixed_shape_tensor_array>;

    namespace detail
    {
        template <>
        struct get_data_type_from_array<sparrow::fixed_shape_tensor_array>
        {
            [[nodiscard]] static constexpr sparrow::data_type get()
            {
                return sparrow::data_type::FIXED_SHAPE_TENSOR;
            }
        };

        // Helper to build JSON metadata for fixed shape tensor
        SPARROW_API std::string make_tensor_metadata_json(
            const std::vector<int64_t>& shape,
            const std::optional<std::vector<std::string>>& dim_names,
            bool column_major
        );

        // Helper to parse JSON metadata from extension metadata
        SPARROW_API void parse_tensor_metadata(
            const std::string& json,
            std::vector<int64_t>& shape,
            std::optional<std::vector<std::string>>& dim_names,
            bool& column_major
        );
    }

    template <>
    struct array_inner_types<fixed_shape_tensor_array> : array_inner_types_base
    {
        using array_type = fixed_shape_tensor_array;
        using inner_value_type = list_value;
        using inner_reference = list_value;
        using inner_const_reference = list_value;
        using value_iterator = functor_index_iterator<detail::layout_value_functor<array_type, inner_value_type>>;
        using const_value_iterator = functor_index_iterator<
            detail::layout_value_functor<const array_type, inner_value_type>>;
        using iterator_tag = std::random_access_iterator_tag;
    };

    /**
     * @brief Array for storing fixed-shape tensors.
     *
     * A fixed-shape tensor array stores multi-dimensional arrays (tensors) where
     * all tensors have the same shape. It uses FixedSizeList as storage format
     * with extension metadata describing the tensor dimensions.
     *
     * Related Apache Arrow specification:
     * - https://arrow.apache.org/docs/format/CanonicalExtensions.html#fixed-shape-tensor
     */
    class SPARROW_API fixed_shape_tensor_array final : public array_bitmap_base<fixed_shape_tensor_array>
    {
    public:

        using self_type = fixed_shape_tensor_array;
        using base_type = array_bitmap_base<self_type>;
        using inner_types = array_inner_types<self_type>;
        using size_type = typename base_type::size_type;

        using bitmap_type = typename base_type::bitmap_type;
        using bitmap_const_reference = typename base_type::bitmap_const_reference;
        using const_bitmap_range = typename base_type::const_bitmap_range;

        using inner_value_type = list_value;
        using inner_reference = list_value;
        using inner_const_reference = list_value;

        using value_type = nullable<inner_value_type>;
        using const_reference = nullable<inner_const_reference, bitmap_const_reference>;
        using iterator_tag = typename base_type::iterator_tag;

        using value_iterator = typename inner_types::value_iterator;
        using const_value_iterator = typename inner_types::const_value_iterator;

        /**
         * @brief Constructs fixed shape tensor array from Arrow proxy.
         *
         * @param proxy Arrow proxy containing fixed shape tensor array data
         *
         * @pre proxy must contain valid Arrow FixedSizeList array with extension metadata
         * @pre proxy extension name must be "arrow.fixed_shape_tensor"
         * @pre proxy must have valid tensor shape in metadata
         * @post Array is initialized with data from proxy
         */
        explicit fixed_shape_tensor_array(arrow_proxy proxy);

        fixed_shape_tensor_array(const self_type&);
        self_type& operator=(const self_type&);

        fixed_shape_tensor_array(self_type&&) noexcept = default;
        self_type& operator=(self_type&&) noexcept = default;

        /**
         * @brief Generic constructor for creating fixed shape tensor array.
         *
         * @tparam ARGS Parameter pack for constructor arguments
         * @param args Constructor arguments (shape, flat_values, validity, etc.)
         *
         * @pre First argument must be the tensor shape (vector<int64_t>)
         * @pre Second argument must be a valid array for flat values
         * @pre flat_values.size() must be divisible by product of shape dimensions
         * @post Array is created with the specified shape and data
         */
        template <class... ARGS>
            requires(mpl::excludes_copy_and_move_ctor_v<fixed_shape_tensor_array, ARGS...>)
        explicit fixed_shape_tensor_array(ARGS&&... args)
            : self_type(create_proxy(std::forward<ARGS>(args)...))
        {
        }

        /**
         * @brief Gets the shape of the tensors.
         *
         * @return Vector of dimension sizes
         *
         * @post Returns the tensor shape extracted from metadata
         */
        [[nodiscard]] const std::vector<int64_t>& shape() const;

        /**
         * @brief Gets the dimension names if available.
         *
         * @return Optional vector of dimension names
         *
         * @post Returns dimension names if specified in metadata, nullopt otherwise
         */
        [[nodiscard]] const std::optional<std::vector<std::string>>& dim_names() const;

        /**
         * @brief Checks if tensors use column-major (Fortran) ordering.
         *
         * @return true if column-major, false if row-major (C-style)
         */
        [[nodiscard]] bool is_column_major() const;

        /**
         * @brief Gets read-only access to the underlying flat array.
         *
         * @return Const pointer to the flat array containing all tensor elements
         */
        [[nodiscard]] const array_wrapper* raw_flat_array() const;

        /**
         * @brief Gets mutable access to the underlying flat array.
         *
         * @return Pointer to the flat array containing all tensor elements
         */
        [[nodiscard]] array_wrapper* raw_flat_array();

    private:

        /**
         * @brief Creates Arrow proxy for fixed shape tensor.
         *
         * @tparam VB Type of validity bitmap input
         * @tparam METADATA_RANGE Type of metadata container
         * @param shape Shape of each tensor (dimensions)
         * @param flat_values Array containing all tensor elements
         * @param validity_input Validity bitmap specification
         * @param dim_names Optional names for each dimension
         * @param column_major Whether to use column-major ordering
         * @param name Optional name for the array
         * @param metadata Optional additional metadata
         * @return Arrow proxy containing the tensor array data and schema
         *
         * @pre shape must not be empty
         * @pre All shape dimensions must be > 0
         * @pre flat_values.size() must be divisible by product of shape
         * @pre If dim_names provided, must have same size as shape
         * @post Returns valid Arrow proxy with FixedSizeList format
         * @post Extension metadata includes tensor configuration
         */
        template <
            validity_bitmap_input VB = validity_bitmap,
            input_metadata_container METADATA_RANGE = std::vector<metadata_pair>>
        [[nodiscard]] static arrow_proxy create_proxy(
            std::vector<int64_t> shape,
            array&& flat_values,
            VB&& validity_input,
            std::optional<std::vector<std::string>> dim_names = std::nullopt,
            bool column_major = false,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<METADATA_RANGE> metadata = std::nullopt
        );

        template <
            validity_bitmap_input VB = validity_bitmap,
            input_metadata_container METADATA_RANGE = std::vector<metadata_pair>>
        [[nodiscard]] static arrow_proxy create_proxy(
            std::vector<int64_t> shape,
            array&& flat_values,
            bool nullable = true,
            std::optional<std::vector<std::string>> dim_names = std::nullopt,
            bool column_major = false,
            std::optional<std::string_view> name = std::nullopt,
            std::optional<METADATA_RANGE> metadata = std::nullopt
        );

        [[nodiscard]] constexpr value_iterator value_begin();
        [[nodiscard]] constexpr value_iterator value_end();
        [[nodiscard]] const_value_iterator value_cbegin() const;
        [[nodiscard]] const_value_iterator value_cend() const;

        [[nodiscard]] inner_reference value(size_type i);
        [[nodiscard]] inner_const_reference value(size_type i) const;

        [[nodiscard]] cloning_ptr<array_wrapper> make_flat_array();
        [[nodiscard]] static int64_t compute_tensor_size(const std::vector<int64_t>& shape);
        void parse_metadata();

        // Convert multi-dimensional index to flat index
        [[nodiscard]] int64_t multi_index_to_flat(const std::vector<int64_t>& indices) const;

        // data members
        cloning_ptr<array_wrapper> p_flat_array;
        std::vector<int64_t> m_shape;
        std::optional<std::vector<std::string>> m_dim_names;
        bool m_column_major = false;
        int64_t m_tensor_size = 0;

        // friend classes
        friend class array_crtp_base<self_type>;
        friend class detail::layout_value_functor<self_type, inner_value_type>;
        friend class detail::layout_value_functor<const self_type, inner_value_type>;
    };

    /****************************************
     * fixed_shape_tensor_array implementation *
     ****************************************/

    template <validity_bitmap_input VB, input_metadata_container METADATA_RANGE>
    arrow_proxy fixed_shape_tensor_array::create_proxy(
        std::vector<int64_t> shape,
        array&& flat_values,
        VB&& validity_input,
        std::optional<std::vector<std::string>> dim_names,
        bool column_major,
        std::optional<std::string_view> name,
        std::optional<METADATA_RANGE> metadata
    )
    {
        const auto tensor_size = compute_tensor_size(shape);
        const auto num_tensors = flat_values.size() / static_cast<size_t>(tensor_size);
        
        validity_bitmap vbitmap = ensure_validity_bitmap(num_tensors, std::forward<VB>(validity_input));
        const auto null_count = vbitmap.null_count();

        auto [flat_arr, flat_schema] = extract_arrow_structures(std::move(flat_values));

        // Create extension metadata
        std::string extension_metadata_json = detail::make_tensor_metadata_json(shape, dim_names, column_major);
        
        // Prepare metadata with extension information
        std::vector<metadata_pair> full_metadata = metadata.has_value()
                                                         ? std::vector<metadata_pair>(metadata->begin(), metadata->end())
                                                         : std::vector<metadata_pair>{};
        full_metadata.emplace_back("ARROW:extension:name", "arrow.fixed_shape_tensor");
        full_metadata.emplace_back("ARROW:extension:metadata", extension_metadata_json);
        
        // Create FixedSizeList schema
        ArrowSchema schema = detail::make_list_arrow_schema(
            "+w:" + std::to_string(tensor_size),
            std::move(flat_schema),
            name,
            std::make_optional(full_metadata),
            true  // nullable
        );

        std::vector<buffer<std::uint8_t>> arr_buffs = {
            std::move(vbitmap).extract_storage()
        };

        ArrowArray arr = detail::make_list_arrow_array(
            static_cast<std::int64_t>(num_tensors),
            static_cast<std::int64_t>(null_count),
            std::move(arr_buffs),
            std::move(flat_arr)
        );

        return arrow_proxy{std::move(arr), std::move(schema)};
    }

    template <validity_bitmap_input VB, input_metadata_container METADATA_RANGE>
    arrow_proxy fixed_shape_tensor_array::create_proxy(
        std::vector<int64_t> shape,
        array&& flat_values,
        bool nullable,
        std::optional<std::vector<std::string>> dim_names,
        bool column_major,
        std::optional<std::string_view> name,
        std::optional<METADATA_RANGE> metadata
    )
    {
        if (nullable)
        {
            return fixed_shape_tensor_array::create_proxy(
                std::move(shape),
                std::move(flat_values),
                validity_bitmap{},
                std::move(dim_names),
                column_major,
                name,
                metadata
            );
        }

        const auto tensor_size = compute_tensor_size(shape);
        const auto num_tensors = flat_values.size() / static_cast<size_t>(tensor_size);
        
        auto [flat_arr, flat_schema] = extract_arrow_structures(std::move(flat_values));

        // Create extension metadata
        std::string extension_metadata_json = detail::make_tensor_metadata_json(shape, dim_names, column_major);
        
        // Prepare metadata with extension information
        std::vector<metadata_pair> full_metadata = metadata.has_value()
                                                         ? std::vector<metadata_pair>(metadata->begin(), metadata->end())
                                                         : std::vector<metadata_pair>{};
        full_metadata.emplace_back("ARROW:extension:name", "arrow.fixed_shape_tensor");
        full_metadata.emplace_back("ARROW:extension:metadata", extension_metadata_json);
        
        // Create FixedSizeList schema
        ArrowSchema schema = detail::make_list_arrow_schema(
            "+w:" + std::to_string(tensor_size),
            std::move(flat_schema),
            name,
            std::make_optional(full_metadata),
            false  // not nullable
        );

        std::vector<buffer<std::uint8_t>> arr_buffs = {
            buffer<std::uint8_t>{nullptr, 0}  // no validity bitmap
        };

        ArrowArray arr = detail::make_list_arrow_array(
            static_cast<std::int64_t>(num_tensors),
            0,  // null_count
            std::move(arr_buffs),
            std::move(flat_arr)
        );

        return arrow_proxy{std::move(arr), std::move(schema)};
    }
}
