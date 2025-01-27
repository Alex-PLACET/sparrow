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

#include <chrono>

#include "sparrow/utils/mp_utils.hpp"
#include "sparrow/utils/packing.hpp"

namespace sparrow
{
    SPARROW_PACKED_STRUCT days_time_interval
    {
        std::chrono::days days;
        std::chrono::milliseconds time;
    }
    SPARROW_PACKED_STRUCT_END;

    SPARROW_PACKED_STRUCT month_day_nanoseconds_interval
    {
        std::chrono::months months;
        std::chrono::days days;
        std::chrono::nanoseconds nanoseconds;
    }
    SPARROW_PACKED_STRUCT_END;

    using interval_types_t = mpl::typelist<std::chrono::months, days_time_interval, month_day_nanoseconds_interval>;

    static constexpr interval_types_t interval_types;
    template <typename T>
    concept interval_type = mpl::contains<T>(interval_types);

}  // namespace sparrow
