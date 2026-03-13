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
 * Typed reference-data commands replicated through the Raft log.
 */
public final class ReferenceDataCommand {
    public enum Type {
        UPSERT_PRODUCT,
        REMOVE_PRODUCT,
        UPSERT_VARIANT,
        REMOVE_VARIANT
    }

    private final Type type;
    private final String productId;
    private final String productName;
    private final String variantId;
    private final String variantName;

    private ReferenceDataCommand(Type type, String productId, String productName, String variantId, String variantName) {
        this.type = type;
        this.productId = blankToEmpty(productId);
        this.productName = blankToEmpty(productName);
        this.variantId = blankToEmpty(variantId);
        this.variantName = blankToEmpty(variantName);
    }

    public static ReferenceDataCommand upsertProduct(String productId, String productName) {
        return new ReferenceDataCommand(Type.UPSERT_PRODUCT, productId, productName, "", "");
    }

    public static ReferenceDataCommand removeProduct(String productId) {
        return new ReferenceDataCommand(Type.REMOVE_PRODUCT, productId, "", "", "");
    }

    public static ReferenceDataCommand upsertVariant(String productId, String variantId, String variantName) {
        return new ReferenceDataCommand(Type.UPSERT_VARIANT, productId, "", variantId, variantName);
    }

    public static ReferenceDataCommand removeVariant(String productId, String variantId) {
        return new ReferenceDataCommand(Type.REMOVE_VARIANT, productId, "", variantId, "");
    }

    public Type type() {
        return type;
    }

    public String productId() {
        return productId;
    }

    public String productName() {
        return productName;
    }

    public String variantId() {
        return variantId;
    }

    public String variantName() {
        return variantName;
    }

    public byte[] encode() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(type.ordinal());
            out.writeUTF(productId);
            out.writeUTF(productName);
            out.writeUTF(variantId);
            out.writeUTF(variantName);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding reference-data command", e);
        }
    }

    public static Optional<ReferenceDataCommand> decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return Optional.empty();
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int ordinal = in.readInt();
            if (ordinal < 0 || ordinal >= Type.values().length) {
                return Optional.empty();
            }
            Type type = Type.values()[ordinal];
            return Optional.of(new ReferenceDataCommand(
                    type,
                    in.readUTF(),
                    in.readUTF(),
                    in.readUTF(),
                    in.readUTF()
            ));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    private static String blankToEmpty(String value) {
        return value == null ? "" : value;
    }
}
