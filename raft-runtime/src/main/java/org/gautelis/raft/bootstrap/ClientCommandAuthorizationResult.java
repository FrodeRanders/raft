/*
 * Copyright (C) 2025-2026 Frode Randers
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
package org.gautelis.raft.bootstrap;

/**
 * Result of an application-level authorization decision for a client write.
 */
public record ClientCommandAuthorizationResult(boolean allowed, String status, String message) {
    public static ClientCommandAuthorizationResult allow() {
        return new ClientCommandAuthorizationResult(true, "AUTHORIZED", "Request authorized");
    }

    public static ClientCommandAuthorizationResult reject(String status, String message) {
        return new ClientCommandAuthorizationResult(false, status, message);
    }
}
