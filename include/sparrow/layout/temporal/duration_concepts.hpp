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

#pragma once

#include "sparrow/types/data_type.hpp"
#include "sparrow/utils/mp_utils.hpp"

namespace sparrow
{
    using duration_types_t = mpl::typelist<
        // std::chrono::duration<int32_t>,
        // std::chrono::duration<int32_t, std::milli>,
        std::chrono::seconds,
        std::chrono::milliseconds,
        std::chrono::microseconds,
        std::chrono::nanoseconds
        // std::chrono::months,
        // std::chrono::day
        >;

    static constexpr duration_types_t duration_types;
    template <typename T>
    concept duration_type = mpl::contains<T>(duration_types);
}  // namespace sparrow
