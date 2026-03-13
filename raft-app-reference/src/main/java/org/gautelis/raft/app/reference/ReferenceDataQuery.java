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
package org.gautelis.raft.app.reference;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;

/**
 * Typed queries for the reference-data application.
 */
public final class ReferenceDataQuery {
    public enum Type {
        VARIANTS_FOR_PRODUCT
    }

    private final Type type;
    private final String productId;

    private ReferenceDataQuery(Type type, String productId) {
        this.type = type;
        this.productId = productId == null ? "" : productId;
    }

    public static ReferenceDataQuery variantsForProduct(String productId) {
        return new ReferenceDataQuery(Type.VARIANTS_FOR_PRODUCT, productId);
    }

    public Type type() {
        return type;
    }

    public String productId() {
        return productId;
    }

    public byte[] encode() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(type.ordinal());
            out.writeUTF(productId);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding reference-data query", e);
        }
    }

    public static Optional<ReferenceDataQuery> decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return Optional.empty();
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int ordinal = in.readInt();
            if (ordinal < 0 || ordinal >= Type.values().length) {
                return Optional.empty();
            }
            return Optional.of(new ReferenceDataQuery(Type.values()[ordinal], in.readUTF()));
        } catch (IOException e) {
            return Optional.empty();
        }
    }
}
