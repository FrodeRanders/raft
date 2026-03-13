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

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Simple demo authorizer that only allows writes from configured requester ids.
 */
public final class AllowListCommandAuthorizer implements ClientCommandAuthorizer {
    private final Set<String> allowedRequesterIds;

    public AllowListCommandAuthorizer(Set<String> allowedRequesterIds) {
        LinkedHashSet<String> normalized = new LinkedHashSet<>();
        if (allowedRequesterIds != null) {
            for (String requesterId : allowedRequesterIds) {
                if (requesterId != null && !requesterId.isBlank()) {
                    normalized.add(requesterId.trim());
                }
            }
        }
        this.allowedRequesterIds = Set.copyOf(normalized);
    }

    @Override
    public ClientCommandAuthorizationResult authorize(ClientCommandAuthorizationContext context) {
        String requesterId = context == null ? "" : safe(context.requesterId());
        if (allowedRequesterIds.contains(requesterId)) {
            return ClientCommandAuthorizationResult.allow();
        }
        return ClientCommandAuthorizationResult.reject(
                "FORBIDDEN",
                "Requester '" + requesterId + "' is not authorized to modify cluster state"
        );
    }

    public static Set<String> parseAllowList(String csv) {
        LinkedHashSet<String> allowed = new LinkedHashSet<>();
        if (csv == null || csv.isBlank()) {
            return allowed;
        }
        for (String token : csv.split(",")) {
            String value = token.trim();
            if (!value.isBlank()) {
                allowed.add(value);
            }
        }
        return allowed;
    }

    private static String safe(String requesterId) {
        if (requesterId == null || requesterId.isBlank()) {
            return "unknown";
        }
        return requesterId.trim().toLowerCase(Locale.ROOT).equals("unknown") ? "unknown" : requesterId.trim();
    }
}
