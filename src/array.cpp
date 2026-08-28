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

#include "sparrow/array.hpp"

#include <stdexcept>
#include <type_traits>
#include <vector>

#include "sparrow/arrow_interface/arrow_array.hpp"
#include "sparrow/arrow_interface/arrow_array_schema_proxy.hpp"
#include "sparrow/arrow_interface/arrow_schema.hpp"
#include "sparrow/layout/array_factory.hpp"
#include "sparrow/layout/array_helper.hpp"
#include "sparrow/utils/contracts.hpp"

namespace sparrow
{
    array::array(ArrowArray&& array, ArrowSchema&& schema)
        : p_array(array_factory(arrow_proxy(std::move(array), std::move(schema))))
    {
    }

    array::array(ArrowArray&& array, ArrowSchema* schema)
        : p_array(array_factory(arrow_proxy(std::move(array), schema)))
    {
    }

    array::array(ArrowArray* array, ArrowSchema* schema)
        : p_array(array_factory(arrow_proxy(array, schema)))
    {
    }

    array::array(arrow_proxy&& ap)
        : p_array(array_factory(std::move(ap)))
    {
    }

    enum data_type array::data_type() const
    {
        return p_array->data_type();
    }

    std::optional<array> array::dictionary() const
    {
        const std::unique_ptr<arrow_proxy>& dict = get_arrow_proxy().dictionary();
        if (dict)
        {
            arrow_proxy& ap = *dict;
            return array{&(ap.array()), &(ap.schema())};
        }
        else
        {
            return std::nullopt;
        }
    }

    std::optional<std::string_view> array::name() const
    {
        return get_arrow_proxy().name();
    }

    void array::set_name(std::optional<std::string_view> name)
    {
        get_arrow_proxy().set_name(name);
    }

    std::optional<key_value_view> array::metadata() const
    {
        return get_arrow_proxy().metadata();
    }

    bool array::empty() const
    {
        return size() == size_type(0);
    }

    array::size_type array::size() const
    {
        return array_size(*p_array);
    }

    array::size_type array::offset() const
    {
        return static_cast<size_type>(get_arrow_proxy().offset());
    }

    std::int64_t array::null_count() const
    {
        return get_arrow_proxy().null_count();
    }

    array::const_reference array::at(size_type index) const
    {
        if (index >= size())
        {
            throw std::out_of_range("array::at: index out of range");
        }
        return array_element(*p_array, index);
    }

    array::const_reference array::operator[](size_type index) const
    {
        return array_element(*p_array, index);
    }

    array::const_reference array::front() const
    {
        SPARROW_ASSERT_TRUE(!empty());
        return (*this)[0];
    }

    array::const_reference array::back() const
    {
        SPARROW_ASSERT_TRUE(!empty());
        return (*this)[size() - 1];
    }

    array array::view() const
    {
        return {get_arrow_proxy().view()};
    }

    bool array::is_view() const
    {
        return get_arrow_proxy().is_view();
    }

    bool array::is_dictionary() const
    {
        return p_array->is_dictionary();
    }

    array array::slice(size_type start, size_type end) const
    {
        SPARROW_ASSERT_TRUE(start <= end);
        return {get_arrow_proxy().slice(start, end)};
    }

    array array::slice_view(size_type start, size_type end) const
    {
        SPARROW_ASSERT_TRUE(start <= end);
        return {get_arrow_proxy().slice_view(start, end)};
    }

    void array::slice_inplace(size_type start, size_type end)
    {
        SPARROW_ASSERT_TRUE(start <= end);
        get_arrow_proxy().slice_inplace(start, end);
    }

    array::iterator array::insert(const_iterator pos, const_iterator first, const_iterator last)
    {
        return insert(pos, first, last, 1);
    }

    array::iterator
    array::insert(const_iterator pos, const_iterator first, const_iterator last, size_type count)
    {
        SPARROW_ASSERT_TRUE(pos.p_object == this);
        SPARROW_ASSERT_TRUE(first.p_object != nullptr);
        SPARROW_ASSERT_TRUE(first.p_object == last.p_object);

        const array& source = *first.p_object;
        const size_type pos_index = pos.m_index;
        const size_type first_index = first.m_index;
        const size_type last_index = last.m_index;

        SPARROW_ASSERT_TRUE(source.data_type() == this->data_type());
        SPARROW_ASSERT_TRUE(pos_index <= size());
        SPARROW_ASSERT_TRUE(first_index <= last_index);
        SPARROW_ASSERT_TRUE(last_index <= source.size());

        if (count == 0 || first_index == last_index)
        {
            return cbegin() + static_cast<iterator::difference_type>(pos_index);
        }

        const auto& destination_wrapper = *p_array;
        const auto& source_wrapper = *source.p_array;
        SPARROW_ASSERT_TRUE(typeid(destination_wrapper) == typeid(source_wrapper));
        visit(
            [this, &source, pos_index, first_index, last_index, count](const auto& array_impl)
            {
                using array_type = std::remove_cvref_t<decltype(array_impl)>;
                array_type& destination = unwrap_array<array_type>(*p_array);
                const array_type& source_array = unwrap_array<array_type>(*source.p_array);

                if constexpr (requires {
                                  destination.insert(
                                      destination.cbegin(),
                                      source_array.cbegin(),
                                      source_array.cbegin()
                                  );
                              })
                {
                    const auto source_first = std::next(
                        source_array.cbegin(),
                        static_cast<std::ptrdiff_t>(first_index)
                    );
                    const auto source_last = std::next(
                        source_array.cbegin(),
                        static_cast<std::ptrdiff_t>(last_index)
                    );
                    using source_input_value = std::remove_cvref_t<decltype(nullable_get(*source_first))>;
                    using destination_input_value = std::remove_cvref_t<
                        decltype(nullable_get(std::declval<typename array_type::value_type&>()))>;
                    constexpr bool can_insert_directly = std::same_as<source_input_value, destination_input_value>;

                    const auto dest_pos = std::next(destination.cbegin(), static_cast<std::ptrdiff_t>(pos_index));

                    if (&destination == &source_array)
                    {
                        // Self-insertion: snapshot first to avoid iterator invalidation.
                        const std::vector<typename array_type::value_type> temp(source_first, source_last);
                        auto current_offset = static_cast<std::ptrdiff_t>(pos_index);
                        for (size_type i = 0; i < count; ++i)
                        {
                            destination.insert(
                                std::next(destination.cbegin(), current_offset),
                                temp.cbegin(),
                                temp.cend()
                            );
                            current_offset += static_cast<std::ptrdiff_t>(temp.size());
                        }
                    }
                    else if constexpr (can_insert_directly)
                    {
                        // Types match, no aliasing: insert directly from source iterators.
                        const auto elem_count = static_cast<std::ptrdiff_t>(last_index - first_index);
                        auto current_offset = static_cast<std::ptrdiff_t>(pos_index);
                        for (size_type i = 0; i < count; ++i)
                        {
                            destination.insert(
                                std::next(destination.cbegin(), current_offset),
                                source_first,
                                source_last
                            );
                            current_offset += elem_count;
                        }
                    }
                    else
                    {
                        // Type mismatch (e.g. nullable<string_view> -> nullable<string>):
                        const auto elem_count = static_cast<std::ptrdiff_t>(last_index - first_index);
                        const auto converting_view = std::ranges::subrange(source_first, source_last)
                                                     | std::views::transform(
                                                         [](const auto& elem) -> typename array_type::value_type
                                                         {
                                                             return typename array_type::value_type(elem);
                                                         }
                                                     );
                        auto current_offset = static_cast<std::ptrdiff_t>(pos_index);
                        for (size_type i = 0; i < count; ++i)
                        {
                            destination.insert(
                                std::next(destination.cbegin(), current_offset),
                                converting_view.begin(),
                                converting_view.end()
                            );
                            current_offset += elem_count;
                        }
                    }
                }
                else
                {
                    SPARROW_ASSERT_TRUE(false);
                }
            }
        );

        return cbegin() + static_cast<iterator::difference_type>(pos_index);
    }

    array::iterator array::erase(const_iterator pos)
    {
        SPARROW_ASSERT_TRUE(pos.p_object == this);
        SPARROW_ASSERT_TRUE(pos.m_index < size());
        return erase(pos, pos + 1);
    }

    array::iterator array::erase(const_iterator first, const_iterator last)
    {
        SPARROW_ASSERT_TRUE(first.p_object == this && last.p_object == this);

        const size_type first_index = first.m_index;
        const size_type last_index = last.m_index;

        SPARROW_ASSERT_TRUE(first_index <= last_index);
        SPARROW_ASSERT_TRUE(last_index <= size());

        const auto count = last_index - first_index;
        if (count == 0)
        {
            return cbegin() + static_cast<iterator::difference_type>(first_index);
        }

        visit(
            [this, first_index, count](const auto& array_impl)
            {
                using array_type = std::remove_cvref_t<decltype(array_impl)>;
                array_type& wrapped_array = unwrap_array<array_type>(*p_array);

                if constexpr (requires { wrapped_array.erase(wrapped_array.cbegin(), wrapped_array.cbegin()); })
                {
                    auto erase_first = std::next(wrapped_array.cbegin(), static_cast<std::ptrdiff_t>(first_index));
                    auto erase_last = std::next(erase_first, static_cast<std::ptrdiff_t>(count));
                    static_cast<void>(wrapped_array.erase(erase_first, erase_last));
                }
                else
                {
                    SPARROW_ASSERT_TRUE(false);
                }
            }
        );

        return cbegin() + static_cast<iterator::difference_type>(first_index);
    }

    namespace
    {
        /**
         * Converts an array value (nullable_variant) into a layout's typed nullable.
         * Preserves nullness; converts convertible values (e.g. nullable<double> to
         * nullable<std::int32_t>); asserts for non-convertible alternatives.
         *
         * VERIFIED FACTS (do not re-investigate):
         * - nullable has a converting copy ctor from other nullable types
         *   (include/sparrow/utils/nullable.hpp:400) used by typed_value(alt)
         * - the null alternative must be special-cased: null_type (data_type.hpp:166)
         *   is an empty struct, not convertible to arithmetic types
         * - nullable(nullval_t) builds a null nullable (nullable.hpp:360)
         * - std::visit instantiates the lambda for EVERY alternative, hence the
         *   requires-guard for non-convertible alternatives (list_value, struct_value...)
         * - conversions are restricted to same-type copies and arithmetic-to-arithmetic:
         *   decimal<__int128>::operator std::string() (decimal.hpp:313) does not compile
         *   on this toolchain, so instantiating nullable<std::string>(nullable<decimal<__int128>>)
         *   would hard-error; exotic cross-family conversions assert instead.
         */
        template <class To, class From>
        concept same_or_arithmetic = std::same_as<To, From>
                                     || (std::is_arithmetic_v<To> && std::is_arithmetic_v<From>);

        template <class typed_value>
        typed_value convert_to_typed(const array::value_type& value)
        {
            return std::visit(
                [](const auto& alt) -> typed_value
                {
                    using alt_type = std::remove_cvref_t<decltype(alt)>;
                    if constexpr (std::same_as<alt_type, nullable<null_type>>)
                    {
                        // Variant-typed layouts (run_end_encoded, dictionary) take the
                        // null alternative directly (exact std::variant alternative);
                        // plain nullable<T> needs the nullval_t constructor.
                        if constexpr (requires { typed_value(alt); })
                        {
                            return typed_value(alt);
                        }
                        else
                        {
                            return typed_value(nullval);
                        }
                    }
                    else if constexpr (requires {
                                          typed_value(alt);
                                          requires same_or_arithmetic<
                                              std::remove_cvref_t<decltype(nullable_get(alt))>,
                                              std::remove_cvref_t<decltype(nullable_get(std::declval<typed_value&>()))>
                                          >;
                                      })
                    {
                        return typed_value(alt);
                    }
                    else
                    {
                        SPARROW_ASSERT_TRUE(false);
                        return typed_value{};
                    }
                },
                value
            );
        }
    }

    void array::push_back(const value_type& value)
    {
        visit(
            [this, &value](const auto& array_impl)
            {
                using array_type = std::remove_cvref_t<decltype(array_impl)>;
                array_type& wrapped_array = unwrap_array<array_type>(*p_array);
                using typed_value = typename array_type::value_type;
                const typed_value converted = convert_to_typed<typed_value>(value);

                // Check the variant-taking overload FIRST: run_end_encoded_array
                // (run_end_encoded_array.hpp:365) defines value_type as the variant
                // itself, so its push_back takes it directly.
                if constexpr (requires { wrapped_array.push_back(value); })
                {
                    wrapped_array.push_back(value);
                }
                else if constexpr (requires { wrapped_array.push_back(converted); })
                {
                    wrapped_array.push_back(converted);
                }
                else if constexpr (requires { wrapped_array.insert(wrapped_array.cend(), converted); })
                {
                    wrapped_array.insert(wrapped_array.cend(), converted);
                }
                else
                {
                    SPARROW_ASSERT_TRUE(false);
                }
            }
        );
    }

    void array::pop_back()
    {
        SPARROW_ASSERT_TRUE(!empty());
        erase(std::prev(cend()));
    }

    void array::resize(size_type new_length)
    {
        resize(new_length, value_type{});
    }

    void array::resize(size_type new_length, const value_type& value)
    {
        visit(
            [this, new_length, &value](const auto& array_impl)
            {
                using array_type = std::remove_cvref_t<decltype(array_impl)>;
                array_type& wrapped_array = unwrap_array<array_type>(*p_array);
                using typed_value = typename array_type::value_type;
                const typed_value converted = convert_to_typed<typed_value>(value);

                // Variant-taking overload first (run_end_encoded_array::resize),
                // then the typed-nullable overload (mutable_array_base::resize).
                if constexpr (requires { wrapped_array.resize(new_length, value); })
                {
                    wrapped_array.resize(new_length, value);
                }
                else if constexpr (requires { wrapped_array.resize(new_length, converted); })
                {
                    wrapped_array.resize(new_length, converted);
                }
                else
                {
                    // ponytail: no native resize on this layout; compose from
                    // erase/push_back. Growth is O(n^2) insert shifts; add a
                    // native resize to the layout if that ever matters.
                    if (new_length < this->size())
                    {
                        this->erase(
                            this->cbegin() + static_cast<iterator::difference_type>(new_length),
                            this->cend()
                        );
                    }
                    else
                    {
                        while (this->size() < new_length)
                        {
                            this->push_back(value);
                        }
                    }
                }
            }
        );
    }

    arrow_proxy& array::get_arrow_proxy()
    {
        return p_array->get_arrow_proxy();
    }

    const arrow_proxy& array::get_arrow_proxy() const
    {
        return p_array->get_arrow_proxy();
    }

    array::iterator array::begin() const
    {
        return cbegin();
    }

    array::iterator array::end() const
    {
        return cend();
    }

    array::const_iterator array::cbegin() const
    {
        return {this, 0};
    }

    array::const_iterator array::cend() const
    {
        return {this, size()};
    }

    bool operator==(const array& lhs, const array& rhs)
    {
        return lhs.visit(
            [&rhs](const auto& typed_lhs) -> bool
            {
                return rhs.visit(
                    [&typed_lhs](const auto& typed_rhs) -> bool
                    {
                        if constexpr (!std::same_as<decltype(typed_lhs), decltype(typed_rhs)>)
                        {
                            return false;
                        }
                        else
                        {
                            return typed_lhs == typed_rhs;
                        }
                    }
                );
            }
        );
    }
}
#if defined(__cpp_lib_format)
auto std::formatter<sparrow::array>::format(const sparrow::array& ar, std::format_context& ctx) const -> iterator
{
    return ar.visit(
        [&ctx](const auto& layout)
        {
            return std::format_to(ctx.out(), "{}", layout);
        }
    );
}
#endif
