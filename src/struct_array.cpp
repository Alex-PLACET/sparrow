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

#include "sparrow/struct_array.hpp"

#include <stdexcept>
#include <vector>

#include "sparrow/debug/copy_tracker.hpp"
#include "sparrow/layout/array_factory.hpp"
#include "sparrow/layout/array_helper.hpp"

namespace sparrow
{
    namespace copy_tracker
    {
        template <>
        SPARROW_API std::string key<struct_array>()
        {
            return "struct_array";
        }
    }

    struct_array::struct_array(arrow_proxy proxy)
        : base_type(std::move(proxy))
        , m_children(make_children())
    {
    }

    struct_array::struct_array(const struct_array& rhs)
        : base_type(rhs)
        , m_children(make_children())
    {
        copy_tracker::increase(copy_tracker::key<struct_array>());
    }

    struct_array& struct_array::operator=(const struct_array& rhs)
    {
        copy_tracker::increase(copy_tracker::key<struct_array>());
        if (this != &rhs)
        {
            base_type::operator=(rhs);
            m_children = make_children();
        }
        return *this;
    }

    auto struct_array::children_count() const -> size_type
    {
        return m_children.size();
    }

    auto struct_array::raw_child(std::size_t i) const -> const array_wrapper*
    {
        SPARROW_ASSERT_TRUE(i < m_children.size());
        return m_children[i].get();
    }

    auto struct_array::raw_child(std::size_t i) -> array_wrapper*
    {
        SPARROW_ASSERT_TRUE(i < m_children.size());
        return m_children[i].get();
    }

    auto struct_array::value_begin() -> value_iterator
    {
        return value_iterator{detail::layout_value_functor<self_type, inner_value_type>{this}, 0};
    }

    auto struct_array::value_end() -> value_iterator
    {
        return {detail::layout_value_functor<self_type, inner_value_type>(this), this->size()};
    }

    auto struct_array::value_cbegin() const -> const_value_iterator
    {
        return {detail::layout_value_functor<const self_type, inner_value_type>(this), 0};
    }

    auto struct_array::value_cend() const -> const_value_iterator
    {
        return const_value_iterator(
            detail::layout_value_functor<const self_type, inner_value_type>(this),
            this->size()
        );
    }

    auto struct_array::value(size_type i) -> inner_reference
    {
        return struct_value{m_children, i};
    }

    auto struct_array::value(size_type i) const -> inner_const_reference
    {
        return struct_value{m_children, i};
    }

    auto struct_array::make_children() -> children_type
    {
        arrow_proxy& proxy = this->get_arrow_proxy();
        children_type children(proxy.children().size(), nullptr);
        for (std::size_t i = 0; i < children.size(); ++i)
        {
            children[i] = array_factory(proxy.children()[i].view());
        }
        return children;
    }

    void struct_array::pop_children(size_t n)
    {
        get_arrow_proxy().pop_children(n);
        m_children = make_children();
    }

    namespace
    {
        using dynamic_value = array::value_type;
    }

    void struct_array::resize_values(size_type new_length, const struct_value& value)
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

    struct_array::value_iterator
    struct_array::insert_value(const_value_iterator pos, const struct_value& value, size_type count)
    {
        const auto index = static_cast<size_type>(std::distance(value_cbegin(), pos));
        if (count == 0)
        {
            return std::next(value_begin(), static_cast<std::ptrdiff_t>(index));
        }
        if (get_arrow_proxy().offset() != 0)
        {
            throw std::logic_error("struct_array::insert_value does not support sliced arrays");
        }

        const size_type child_count = children_count();
        const size_type value_count = value.size();
        const bool is_null_value = value_count == 0 && child_count != 0;
        if (!is_null_value && value_count != child_count)
        {
            throw std::invalid_argument("Struct value has an incompatible number of fields");
        }

        std::vector<dynamic_value> inserted_values(child_count);
        for (size_type child_index = 0; child_index < child_count; ++child_index)
        {
            inserted_values[child_index] = is_null_value
                                               ? array_default_value(*m_children[child_index])
                                               : array_materialize_element(value[child_index]);
        }

        const size_type old_size = this->size();
        rebuild_children(
            old_size + count,
            [&](std::vector<dynamic_value>& new_values, size_type child_index)
            {
                new_values.insert(
                    new_values.begin() + static_cast<std::ptrdiff_t>(index),
                    count,
                    inserted_values[child_index]
                );
            }
        );
        return std::next(value_begin(), static_cast<std::ptrdiff_t>(index));
    }

    struct_array::value_iterator struct_array::erase_values(const_value_iterator pos, size_type count)
    {
        const auto index = static_cast<size_type>(std::distance(value_cbegin(), pos));
        if (count == 0)
        {
            return std::next(value_begin(), static_cast<std::ptrdiff_t>(index));
        }
        if (get_arrow_proxy().offset() != 0)
        {
            throw std::logic_error("struct_array::erase_values does not support sliced arrays");
        }

        const size_type old_size = this->size();
        rebuild_children(
            old_size - count,
            [&](std::vector<dynamic_value>& new_values, size_type)
            {
                const auto first = new_values.begin() + static_cast<std::ptrdiff_t>(index);
                new_values.erase(first, first + static_cast<std::ptrdiff_t>(count));
            }
        );
        return std::next(value_begin(), static_cast<std::ptrdiff_t>(index));
    }

}

#if defined(__cpp_lib_format)

auto std::formatter<sparrow::struct_array>::format(
    const sparrow::struct_array& struct_array,
    std::format_context& ctx
) const -> decltype(ctx.out())
{
    const auto get_names = [](const sparrow::struct_array& sa) -> std::vector<std::string>
    {
        std::vector<std::string> names;
        names.reserve(sa.children_count());
        for (std::size_t i = 0; i < sa.children_count(); ++i)
        {
            names.emplace_back(sa.raw_child(i)->get_arrow_proxy().name().value_or("N/A"));
        }
        return names;
    };

    const size_t member_count = struct_array.at(0).get().size();
    const auto result = std::views::iota(0u, member_count)
                        | std::ranges::views::transform(
                            [&struct_array](const auto index)
                            {
                                return std::ranges::views::transform(
                                    struct_array,
                                    [index](const auto& ref) -> sparrow::array_traits::const_reference
                                    {
                                        if (ref.has_value())
                                        {
                                            return ref.value()[index];
                                        }
                                        return {};
                                    }
                                );
                            }
                        );

    sparrow::to_table_with_columns(ctx.out(), get_names(struct_array), result);
    return ctx.out();
}

namespace sparrow
{
    std::ostream& operator<<(std::ostream& os, const struct_array& value)
    {
        os << std::format("{}", value);
        return os;
    }
}

#endif
