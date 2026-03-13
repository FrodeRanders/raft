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

import java.util.Objects;

/**
 * Simple optional authenticator for demos and controlled service-to-service use.
 */
public final class SharedSecretCommandAuthenticator implements ClientCommandAuthenticator {
    public static final String SCHEME = "shared-secret";

    private final String expectedSecret;

    public SharedSecretCommandAuthenticator(String expectedSecret) {
        if (expectedSecret == null || expectedSecret.isBlank()) {
            throw new IllegalArgumentException("Shared secret must be non-blank");
        }
        this.expectedSecret = expectedSecret;
    }

    @Override
    public ClientCommandAuthenticationResult authenticate(ClientCommandAuthenticationContext context) {
        String scheme = context == null ? "" : blankToEmpty(context.authenticationScheme());
        String token = context == null ? "" : blankToEmpty(context.authenticationToken());
        String requesterId = context == null ? "unknown" : blankToUnknown(context.requesterId());

        if (!SCHEME.equalsIgnoreCase(scheme)) {
            return ClientCommandAuthenticationResult.reject(
                    "UNAUTHENTICATED",
                    "Client command authentication requires scheme '" + SCHEME + "'"
            );
        }
        if (!Objects.equals(expectedSecret, token)) {
            return ClientCommandAuthenticationResult.reject(
                    "UNAUTHENTICATED",
                    "Client command authentication failed"
            );
        }
        return ClientCommandAuthenticationResult.authenticated(new AuthenticatedPrincipal(requesterId, SCHEME));
    }

    private static String blankToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    private static String blankToUnknown(String value) {
        return value == null || value.isBlank() ? "unknown" : value.trim();
    }
}
