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

#include <algorithm>
#include <optional>
#include <ranges>
#include <span>
#include <string_view>
#include <type_traits>

#include "sparrow/allocator.hpp"
#include "sparrow/any_data.hpp"
#include "sparrow/arrow_array_schema_utils.hpp"
#include "sparrow/c_interface.hpp"
#include "sparrow/contracts.hpp"

#include "mp_utils.hpp"

namespace sparrow
{
    inline void arrow_schema_custom_deleter(ArrowSchema* schema)
    {
        if (schema)
        {
            if (schema->release != nullptr)
            {
                schema->release(schema);
            }
            delete schema;
        }
    }

    /**
     * A custom deleter for ArrowSchema.
     */
    struct arrow_schema_custom_deleter_struct
    {
        void operator()(ArrowSchema* schema) const
        {
            arrow_schema_custom_deleter(schema);
        }
    };

    /**
     * A unique pointer to an ArrowSchema with a custom deleter.
     * It must be used to manage ArrowSchema objects.
     */
    using arrow_schema_unique_ptr = std::unique_ptr<ArrowSchema, arrow_schema_custom_deleter_struct>;

    /// Shared pointer to an ArrowSchema. Must be used to manage the memory of an ArrowSchema.
    class arrow_schema_shared_ptr : public std::shared_ptr<ArrowSchema>
    {
        explicit arrow_schema_shared_ptr()
            : std::shared_ptr<ArrowSchema>(nullptr, arrow_schema_custom_deleter)
        {
        }

        explicit arrow_schema_shared_ptr(arrow_schema_shared_ptr&& ptr)
            : std::shared_ptr<ArrowSchema>(std::move(ptr))
        {
        }

        explicit arrow_schema_shared_ptr(std::nullptr_t) noexcept
            : std::shared_ptr<ArrowSchema>(nullptr, arrow_schema_custom_deleter)
        {
        }
    };

    /**
     * Struct representing private data for ArrowSchema.
     *
     * This struct holds the private data for ArrowSchema, including format,
     * name and metadata strings, children, and dictionary. It is used in the
     * Sparrow library.
     */
    struct arrow_schema_private_data

    {
        arrow_schema_private_data() = delete;
        arrow_schema_private_data(const arrow_schema_private_data&) = delete;
        arrow_schema_private_data(arrow_schema_private_data&&) = delete;
        arrow_schema_private_data& operator=(const arrow_schema_private_data&) = delete;
        arrow_schema_private_data& operator=(arrow_schema_private_data&&) = delete;

        template <mpl::string_like F, mpl::string_like_or_null N, mpl::string_like_or_null M, class C, class D>
        explicit arrow_schema_private_data(F format, N name, M metadata, C children, D dictionary);

        ~arrow_schema_private_data();

        [[nodiscard]] const char* format() const noexcept;
        [[nodiscard]] const char* name() const noexcept;
        [[nodiscard]] const char* metadata() const noexcept;
        [[nodiscard]] ArrowSchema** children() noexcept;
        [[nodiscard]] ArrowSchema* dictionary() noexcept;

    private:

        any_data m_format;
        any_data m_name;
        any_data m_metadata;
        any_data_container m_children;
        any_data m_dictionary;
    };

    template <mpl::string_like F, mpl::string_like_or_null N, mpl::string_like_or_null M, class C, class D>
    arrow_schema_private_data::arrow_schema_private_data(F format, N name, M metadata, C children, D dictionary)
        : m_format(std::forward<F>(format))
        , m_name(std::forward<N>(name))
        , m_metadata(std::forward<M>(metadata))
        , m_children(std::forward<C>(children))
        , m_dictionary(std::forward<D>(dictionary))
    {
    }

    arrow_schema_private_data::~arrow_schema_private_data()
    {
        if (m_children.owns_data())
        {
            for (auto& child : m_children.get_pointers_vec<ArrowSchema>())
            {
                SPARROW_ASSERT_TRUE(child->release == nullptr)
                if (child->release != nullptr)
                {
                    child->release(child);
                }
            }
        }

        auto dictionary = m_dictionary.get<ArrowSchema>();
        if (dictionary != nullptr)
        {
            SPARROW_ASSERT_TRUE(dictionary->release == nullptr)
            if (dictionary->release != nullptr)
            {
                dictionary->release(dictionary);
            }
        }
    }

    [[nodiscard]] inline const char* arrow_schema_private_data::format() const noexcept
    {
        return m_format.get<char>();
    }

    [[nodiscard]] inline const char* arrow_schema_private_data::name() const noexcept
    {
        return m_name.get<char>();
    }

    [[nodiscard]] inline const char* arrow_schema_private_data::metadata() const noexcept
    {
        return m_metadata.get<char>();
    }

    [[nodiscard]] inline ArrowSchema** arrow_schema_private_data::children() noexcept
    {
        return m_children.get<ArrowSchema>();
    }

    [[nodiscard]] ArrowSchema* arrow_schema_private_data::dictionary() noexcept
    {
        return m_dictionary.get<ArrowSchema>();
    }

    /**
     * Deletes an ArrowSchema.
     *
     * @tparam Allocator The allocator for the strings of the ArrowSchema.
     * @param schema The ArrowSchema to delete. Should be a valid pointer to an ArrowSchema. Its release
     * callback should be set.
     */
    void delete_schema(ArrowSchema* schema)
    {
        SPARROW_ASSERT_FALSE(schema == nullptr)
        SPARROW_ASSERT_TRUE(schema->release == std::addressof(delete_schema))

        schema->flags = 0;
        schema->n_children = 0;
        schema->children = nullptr;
        schema->dictionary = nullptr;
        schema->name = nullptr;
        schema->format = nullptr;
        schema->metadata = nullptr;
        if (schema->private_data != nullptr)
        {
            const auto private_data = static_cast<arrow_schema_private_data*>(schema->private_data);
            delete private_data;
        }
        schema->private_data = nullptr;
        schema->release = nullptr;
    }

    inline arrow_schema_unique_ptr default_arrow_schema()
    {
        auto ptr = arrow_schema_unique_ptr(new ArrowSchema());
        ptr->format = nullptr;
        ptr->name = nullptr;
        ptr->metadata = nullptr;
        ptr->flags = 0;
        ptr->n_children = 0;
        ptr->children = nullptr;
        ptr->dictionary = nullptr;
        ptr->release = nullptr;
        ptr->private_data = nullptr;
        return ptr;
    }

    // template function to check if a string_like is empty
    template <mpl::string_like T>
    constexpr bool is_empty(const T& str)
    {
        if constexpr (std::is_same_v<T, std::string>)
        {
            return str.empty();
        }
        else if constexpr (std::is_same_v<T, std::string_view>)
        {
            return str.empty();
        }
        else if constexpr (std::is_same_v<T, const char*> || std::is_same_v<T, char*>)
        {
            return str[0] == '\0';
        }
        else if constexpr (std::is_same_v<T, const char* const> || std::is_same_v<T, char* const>)
        {
            return str[0] == '\0';
        }
        else if constexpr (std::is_same_v<T, std::vector<char>>)
        {
            return str.empty();
        }
    }

    /**
     * Creates an ArrowSchema.
     *
     * @tparam Allocator The allocator for the strings of the ArrowSchema.
     * @tparam C An optional (nullptr) container of ArrowSchema pointers/objects/reference.
     * @tparam D An optional (nullptr) object/reference/pointer ArrowSchema.
     * @param format A mandatory, null-terminated, UTF8-encoded string describing the data type. If the data
     *               type is nested, child types are not encoded here but in the ArrowSchema.children
     *               structures.
     * @param name An optional (nullptr), null-terminated, UTF8-encoded string of the field or array name.
     *             This is mainly used to reconstruct child fields of nested types.
     * @param metadata An optional (nullptr), binary string describing the type’s metadata. If the data type
     *                 is nested, the metadata for child types are not encoded here but in the
     * ArrowSchema.children structures.
     * @param flags A bitfield of flags enriching the type description. Its value is computed by OR’ing
     *              together the flag values.
     * @param children An optional (nullptr), container of ArrowSchema pointers/objects/reference. Children
     * pointers must not be null.
     * @param dictionary An optional (nullptr) object/reference/pointer ArrowSchema to the type of dictionary
     * values. Must be present if the ArrowSchema represents a dictionary-encoded type. Must be nullptr
     * otherwise.
     * @return The created ArrowSchema unique pointer.
     */
    template <mpl::string_like F, mpl::string_like_or_null N, mpl::string_like_or_null M, class C, class D>
    arrow_schema_unique_ptr
    make_arrow_schema(F format, N name, M metadata, std::optional<ArrowFlag> flags, C children, D dictionary)
    {
        SPARROW_ASSERT_FALSE(is_empty(format))
        SPARROW_ASSERT_TRUE(std::ranges::none_of(
            children,
            [](const auto& child)
            {
                return child == nullptr;
            }
        ))

        arrow_schema_unique_ptr schema = default_arrow_schema();

        schema->flags = flags.has_value() ? static_cast<int64_t>(flags.value()) : 0;
        schema->n_children = static_cast<int64_t>(children.size());

        schema->private_data = new arrow_schema_private_data(
            std::move(format),
            std::move(name),
            std::move(metadata),
            std::move(children),
            std::move(dictionary)
        );

        const auto private_data = static_cast<arrow_schema_private_data*>(schema->private_data);
        schema->format = private_data->format();
        schema->name = private_data->name();
        schema->metadata = private_data->metadata();
        schema->children = private_data->children();
        schema->dictionary = private_data->dictionary();
        schema->release = delete_schema;
        return schema;
    };
}
