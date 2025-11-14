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

#include <sstream>
#include <stdexcept>

#include "sparrow/arrow_interface/arrow_array.hpp"
#include "sparrow/arrow_interface/arrow_schema.hpp"
#include "sparrow/buffer/dynamic_bitset/dynamic_bitset.hpp"
#include "sparrow/fixed_shape_tensor_array.hpp"
#include "sparrow/layout/array_factory.hpp"
#include "sparrow/layout/layout_utils.hpp"
#include "sparrow/utils/contracts.hpp"
#include "sparrow/utils/repeat_container.hpp"

namespace sparrow
{
    namespace detail
    {
        std::string make_tensor_metadata_json(
            const std::vector<int64_t>& shape,
            const std::optional<std::vector<std::string>>& dim_names,
            bool column_major
        )
        {
            std::ostringstream json;
            json << "{\"shape\":[";
            for (size_t i = 0; i < shape.size(); ++i)
            {
                if (i > 0)
                    json << ",";
                json << shape[i];
            }
            json << "]";

            if (dim_names.has_value())
            {
                json << ",\"dim_names\":[";
                for (size_t i = 0; i < dim_names->size(); ++i)
                {
                    if (i > 0)
                        json << ",";
                    json << "\"" << (*dim_names)[i] << "\"";
                }
                json << "]";
            }

            if (column_major)
            {
                json << ",\"permutation\":[";
                for (size_t i = 0; i < shape.size(); ++i)
                {
                    if (i > 0)
                        json << ",";
                    json << i;
                }
                json << "]";
            }

            json << "}";
            return json.str();
        }

        void parse_tensor_metadata(
            const std::string& json,
            std::vector<int64_t>& shape,
            std::optional<std::vector<std::string>>& dim_names,
            bool& column_major
        )
        {
            // Simple JSON parser for tensor metadata
            // Format: {"shape":[3,4],"dim_names":["x","y"],"permutation":[0,1]}

            shape.clear();
            dim_names = std::nullopt;
            column_major = false;

            // Find shape array
            auto shape_pos = json.find("\"shape\":[");
            if (shape_pos == std::string::npos)
            {
                throw std::runtime_error("Invalid tensor metadata: missing 'shape' field");
            }

            auto shape_start = json.find('[', shape_pos);
            auto shape_end = json.find(']', shape_start);
            if (shape_start == std::string::npos || shape_end == std::string::npos)
            {
                throw std::runtime_error("Invalid tensor metadata: malformed 'shape' array");
            }

            // Parse shape values
            std::string shape_str = json.substr(shape_start + 1, shape_end - shape_start - 1);
            std::istringstream shape_stream(shape_str);
            std::string dim;
            while (std::getline(shape_stream, dim, ','))
            {
                shape.push_back(std::stoll(dim));
            }

            // Find dim_names array (optional)
            auto dim_names_pos = json.find("\"dim_names\":[");
            if (dim_names_pos != std::string::npos)
            {
                auto dim_names_start = json.find('[', dim_names_pos);
                auto dim_names_end = json.find(']', dim_names_start);
                if (dim_names_start != std::string::npos && dim_names_end != std::string::npos)
                {
                    std::vector<std::string> names;
                    auto current_pos = dim_names_start + 1;
                    while (current_pos < dim_names_end)
                    {
                        auto quote_start = json.find('"', current_pos);
                        if (quote_start >= dim_names_end)
                            break;
                        auto quote_end = json.find('"', quote_start + 1);
                        if (quote_end >= dim_names_end)
                            break;
                        names.push_back(json.substr(quote_start + 1, quote_end - quote_start - 1));
                        current_pos = quote_end + 1;
                    }
                    if (!names.empty())
                    {
                        dim_names = std::move(names);
                    }
                }
            }

            // Check for permutation (indicates column-major)
            column_major = json.find("\"permutation\":[") != std::string::npos;
        }
    }

    /********************************************
     * fixed_shape_tensor_array implementation  *
     ********************************************/

    fixed_shape_tensor_array::fixed_shape_tensor_array(arrow_proxy proxy)
        : base_type(std::move(proxy))
        , p_flat_array(make_flat_array())
    {
        parse_metadata();
        m_tensor_size = compute_tensor_size(m_shape);
    }

    fixed_shape_tensor_array::fixed_shape_tensor_array(const self_type& rhs)
        : base_type(rhs)
        , p_flat_array(make_flat_array())
        , m_shape(rhs.m_shape)
        , m_dim_names(rhs.m_dim_names)
        , m_column_major(rhs.m_column_major)
        , m_tensor_size(rhs.m_tensor_size)
    {
    }

    auto fixed_shape_tensor_array::operator=(const self_type& rhs) -> self_type&
    {
        if (this != &rhs)
        {
            base_type::operator=(rhs);
            p_flat_array = make_flat_array();
            m_shape = rhs.m_shape;
            m_dim_names = rhs.m_dim_names;
            m_column_major = rhs.m_column_major;
            m_tensor_size = rhs.m_tensor_size;
        }
        return *this;
    }

    const std::vector<int64_t>& fixed_shape_tensor_array::shape() const
    {
        return m_shape;
    }

    const std::optional<std::vector<std::string>>& fixed_shape_tensor_array::dim_names() const
    {
        return m_dim_names;
    }

    bool fixed_shape_tensor_array::is_column_major() const
    {
        return m_column_major;
    }

    const array_wrapper* fixed_shape_tensor_array::raw_flat_array() const
    {
        return p_flat_array.get();
    }

    array_wrapper* fixed_shape_tensor_array::raw_flat_array()
    {
        return p_flat_array.get();
    }

    constexpr auto fixed_shape_tensor_array::value_begin() -> value_iterator
    {
        return value_iterator(detail::layout_value_functor<self_type, inner_value_type>(this), 0);
    }

    constexpr auto fixed_shape_tensor_array::value_end() -> value_iterator
    {
        return value_iterator(
            detail::layout_value_functor<self_type, inner_value_type>(this),
            this->size()
        );
    }

    auto fixed_shape_tensor_array::value_cbegin() const -> const_value_iterator
    {
        return const_value_iterator(
            detail::layout_value_functor<const self_type, inner_value_type>(this),
            0
        );
    }

    auto fixed_shape_tensor_array::value_cend() const -> const_value_iterator
    {
        return const_value_iterator(
            detail::layout_value_functor<const self_type, inner_value_type>(this),
            this->size()
        );
    }

    auto fixed_shape_tensor_array::value(size_type i) -> inner_reference
    {
        const auto offset = i * m_tensor_size;
        return list_value{p_flat_array.get(), offset, offset + static_cast<size_type>(m_tensor_size)};
    }

    auto fixed_shape_tensor_array::value(size_type i) const -> inner_const_reference
    {
        const auto offset = i * m_tensor_size;
        return list_value{p_flat_array.get(), offset, offset + static_cast<size_type>(m_tensor_size)};
    }    cloning_ptr<array_wrapper> fixed_shape_tensor_array::make_flat_array()
    {
        return array_factory(this->get_arrow_proxy().children()[0].view());
    }

    int64_t fixed_shape_tensor_array::compute_tensor_size(const std::vector<int64_t>& shape)
    {
        return std::accumulate(shape.begin(), shape.end(), int64_t{1}, std::multiplies<>());
    }

    void fixed_shape_tensor_array::parse_metadata()
    {
        const auto& proxy_metadata = get_arrow_proxy().metadata();

        if (!proxy_metadata.has_value())
        {
            throw std::runtime_error("Fixed shape tensor array requires extension metadata");
        }

        // Find extension metadata
        auto it = proxy_metadata->find("ARROW:extension:metadata");
        if (it == proxy_metadata->end())
        {
            throw std::runtime_error("Fixed shape tensor array missing extension metadata");
        }

        const auto [key, value] = *it;
        detail::parse_tensor_metadata(std::string(value), m_shape, m_dim_names, m_column_major);

        if (m_shape.empty())
        {
            throw std::runtime_error("Fixed shape tensor array has empty shape");
        }
    }

    int64_t fixed_shape_tensor_array::multi_index_to_flat(const std::vector<int64_t>& indices) const
    {
        int64_t flat_idx = 0;
        int64_t stride = 1;

        if (m_column_major)
        {
            // Column-major (Fortran-style): first dimension varies fastest
            for (size_t i = 0; i < indices.size(); ++i)
            {
                SPARROW_ASSERT(indices[i] >= 0 && indices[i] < m_shape[i], "Index out of bounds");
                flat_idx += indices[i] * stride;
                stride *= m_shape[i];
            }
        }
        else
        {
            // Row-major (C-style): last dimension varies fastest
            for (int i = static_cast<int>(indices.size()) - 1; i >= 0; --i)
            {
                SPARROW_ASSERT(
                    indices[i] >= 0 && indices[i] < m_shape[i],
                    "Index out of bounds"
                );
                flat_idx += indices[i] * stride;
                stride *= m_shape[i];
            }
        }

        return flat_idx;
    }
}
