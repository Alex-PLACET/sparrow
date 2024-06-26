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

#include <array>
#include <memory>
#include <optional>
#include <string_view>

#include "sparrow/c_interface.hpp"

#include "doctest/doctest.h"

TEST_SUITE("C Data Interface")
{
    TEST_CASE("ArrowArray")
    {
        SUBCASE("make_array_constructor")
        {
            std::vector<sparrow::arrow_array_unique_ptr> children;
            children.emplace_back(sparrow::default_arrow_array());
            children.emplace_back(sparrow::default_arrow_array());
            const auto children_1_ptr = children[0].get();
            const auto children_2_ptr = children[1].get();

            sparrow::arrow_array_unique_ptr dictionary(sparrow::default_arrow_array());
            const auto dictionary_ptr = dictionary.get();

            std::vector<size_t> buffers_sizes = {1};
            const auto array = sparrow::make_arrow_array<int, std::allocator>(
                1,
                0,
                0,
                buffers_sizes,
                std::move(children),
                std::move(dictionary)
            );
            CHECK_EQ(array->length, 1);
            CHECK_EQ(array->null_count, 0);
            CHECK_EQ(array->offset, 0);
            CHECK_EQ(array->n_buffers, 1);
            CHECK_EQ(array->n_children, 2);
            CHECK_NE(array->buffers, nullptr);
            REQUIRE_NE(array->children, nullptr);
            CHECK_EQ(array->children[0], children_1_ptr);
            CHECK_EQ(array->children[1], children_2_ptr);
            CHECK_EQ(array->dictionary, dictionary_ptr);
            CHECK_EQ(array->release, sparrow::delete_array<int, std::allocator>);
            CHECK_NE(array->private_data, nullptr);
        }

        SUBCASE("make_array_constructor no children and dictionary")
        {
            std::vector<sparrow::arrow_array_unique_ptr> children;
            const auto array = sparrow::make_arrow_array<int, std::allocator>(
                1,
                0,
                0,
                std::array<size_t, 1>{1},
                std::move(children),
                sparrow::arrow_array_unique_ptr()
            );
            CHECK_EQ(array->length, 1);
            CHECK_EQ(array->null_count, 0);
            CHECK_EQ(array->offset, 0);
            CHECK_EQ(array->n_buffers, 1);
            CHECK_EQ(array->n_children, 0);
            CHECK_NE(array->buffers, nullptr);
            CHECK_EQ(array->children, nullptr);
            CHECK_EQ(array->dictionary, nullptr);
            CHECK_EQ(array->release, sparrow::delete_array<int, std::allocator>);
            CHECK_NE(array->private_data, nullptr);
        }

        SUBCASE("ArrowArray release")
        {
            std::vector<sparrow::arrow_array_unique_ptr> children;
            children.emplace_back(sparrow::default_arrow_array());
            children.emplace_back(sparrow::default_arrow_array());
            sparrow::arrow_array_unique_ptr dictionary(sparrow::default_arrow_array());
            auto array = sparrow::make_arrow_array<int, std::allocator>(
                1,
                0,
                0,
                std::array<size_t, 1>{1},
                std::move(children),
                std::move(dictionary)
            );

            array->release(array.get());

            CHECK_EQ(array->length, 0);
            CHECK_EQ(array->null_count, 0);
            CHECK_EQ(array->offset, 0);
            CHECK_EQ(array->n_buffers, 0);
            CHECK_EQ(array->n_children, 0);
            CHECK_EQ(array->buffers, nullptr);
            CHECK_EQ(array->children, nullptr);
            CHECK_EQ(array->release, nullptr);
            CHECK_EQ(array->private_data, nullptr);
        }

        SUBCASE("ArrowArray release no children and dictionary")
        {
            std::vector<sparrow::arrow_array_unique_ptr> children;
            auto array = sparrow::make_arrow_array<int, std::allocator>(
                1,
                0,
                0,
                std::array<size_t, 1>{1},
                std::move(children),
                sparrow::arrow_array_unique_ptr()
            );

            array->release(array.get());

            CHECK_EQ(array->length, 0);
            CHECK_EQ(array->null_count, 0);
            CHECK_EQ(array->offset, 0);
            CHECK_EQ(array->n_buffers, 0);
            CHECK_EQ(array->n_children, 0);
            CHECK_EQ(array->buffers, nullptr);
            CHECK_EQ(array->children, nullptr);
            CHECK_EQ(array->release, nullptr);
            CHECK_EQ(array->private_data, nullptr);
        }
    }


    TEST_CASE("ArrowSchema")
    {
        SUBCASE("make_schema_constructor")
        {
            std::vector<sparrow::arrow_schema_unique_ptr> children;
            children.emplace_back(sparrow::default_arrow_schema());
            children.emplace_back(sparrow::default_arrow_schema());

            const auto children_1_ptr = children[0].get();
            const auto children_2_ptr = children[1].get();

            sparrow::arrow_schema_unique_ptr dictionary(sparrow::default_arrow_schema());
            const auto dictionary_ptr = dictionary.get();

            constexpr std::string_view format = "format";
            constexpr std::string_view name = "name";
            std::vector<char> metadata = {'\0', '\0', '\0', '\0'};
            const auto schema = sparrow::make_arrow_schema<std::allocator>(
                format,
                name,
                metadata,
                sparrow::ArrowFlag::DICTIONARY_ORDERED,
                std::move(children),
                std::move(dictionary)
            );

            const auto schema_format = std::string_view(schema->format);
            const bool format_eq = schema_format == format;
            CHECK(format_eq);
            const auto schema_name = std::string_view(schema->name);
            const bool name_eq = schema_name == name;
            CHECK(name_eq);
            CHECK_EQ(schema->metadata[0], metadata[0]);
            CHECK_EQ(schema->metadata[1], metadata[1]);
            CHECK_EQ(schema->metadata[2], metadata[2]);
            CHECK_EQ(schema->metadata[3], metadata[3]);
            CHECK_EQ(schema->flags, 1);
            CHECK_EQ(schema->n_children, 2);
            REQUIRE_NE(schema->children, nullptr);
            CHECK_EQ(schema->children[0], children_1_ptr);
            CHECK_EQ(schema->children[1], children_2_ptr);
            CHECK_EQ(schema->dictionary, dictionary_ptr);
            CHECK_EQ(schema->release, sparrow::delete_schema<std::allocator>);
            CHECK_NE(schema->private_data, nullptr);
        }

        SUBCASE("make_schema_constructor no children, no dictionary, no name and metadata")
        {
            std::vector<sparrow::arrow_schema_unique_ptr> children;
            const auto schema = sparrow::make_arrow_schema<std::allocator>(
                "format",
                std::string_view(),
                std::nullopt,
                sparrow::ArrowFlag::DICTIONARY_ORDERED,
                std::move(children),
                sparrow::arrow_schema_unique_ptr()
            );

            const auto schema_format = std::string_view(schema->format);
            const bool format_eq = schema_format == "format";
            CHECK(format_eq);
            CHECK_EQ(schema->name, nullptr);
            CHECK_EQ(schema->metadata, nullptr);
            CHECK_EQ(schema->flags, 1);
            CHECK_EQ(schema->n_children, 0);
            CHECK_EQ(schema->children, nullptr);
            CHECK_EQ(schema->dictionary, nullptr);
            CHECK_EQ(schema->release, sparrow::delete_schema<std::allocator>);
            CHECK_NE(schema->private_data, nullptr);
        }

        SUBCASE("ArrowSchema release")
        {
            std::vector<sparrow::arrow_schema_unique_ptr> children;
            children.emplace_back(sparrow::default_arrow_schema());
            children.emplace_back(sparrow::default_arrow_schema());
            sparrow::arrow_schema_unique_ptr dictionary(sparrow::default_arrow_schema());

            std::vector<char> metadata = {'\0', '\0', '\0', '\0'};

            auto schema = sparrow::make_arrow_schema<std::allocator>(
                "format",
                "name",
                metadata,
                sparrow::ArrowFlag::DICTIONARY_ORDERED,
                std::move(children),
                std::move(dictionary)
            );

            schema->release(schema.get());

            CHECK_EQ(schema->format, nullptr);
            CHECK_EQ(schema->name, nullptr);
            CHECK_EQ(schema->metadata, nullptr);
            CHECK_EQ(schema->children, nullptr);
            CHECK_EQ(schema->dictionary, nullptr);
            CHECK_EQ(schema->release, nullptr);
            CHECK_EQ(schema->private_data, nullptr);
        }

        SUBCASE("ArrowSchema release no children, no dictionary, no name and metadata")
        {
            std::vector<sparrow::arrow_schema_unique_ptr> children;
            auto schema = sparrow::make_arrow_schema<std::allocator>(
                "format",
                std::string_view(),
                std::nullopt,
                sparrow::ArrowFlag::DICTIONARY_ORDERED,
                std::move(children),
                sparrow::arrow_schema_unique_ptr()
            );

            schema->release(schema.get());

            CHECK_EQ(schema->format, nullptr);
            CHECK_EQ(schema->name, nullptr);
            CHECK_EQ(schema->metadata, nullptr);
            CHECK_EQ(schema->children, nullptr);
            CHECK_EQ(schema->dictionary, nullptr);
            CHECK_EQ(schema->release, nullptr);
            CHECK_EQ(schema->private_data, nullptr);
        }
    }
}
