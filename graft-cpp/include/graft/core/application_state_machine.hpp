/*
 * Copyright (C) 2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace graft {
    class ApplicationStateMachine {
    public:
        virtual ~ApplicationStateMachine() = default;

        virtual std::string apply(std::int64_t index, std::int64_t term, std::string_view command) = 0;

        virtual std::string query(std::string_view request) const = 0;

        virtual std::string snapshot() const = 0;

        virtual void restore(std::string_view snapshot) = 0;
    };
} // namespace graft
