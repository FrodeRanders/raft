/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Query result payloads for the reference-data application.
 */
public final class ReferenceDataQueryResult {
    public record VariantView(String variantId, String variantName) {
    }

    private final String productId;
    private final String productName;
    private final boolean found;
    private final List<VariantView> variants;

    private ReferenceDataQueryResult(String productId, String productName, boolean found, List<VariantView> variants) {
        this.productId = productId == null ? "" : productId;
        this.productName = productName == null ? "" : productName;
        this.found = found;
        this.variants = variants == null ? List.of() : List.copyOf(variants);
    }

    public static ReferenceDataQueryResult variantsForProduct(String productId, String productName, boolean found, List<VariantView> variants) {
        return new ReferenceDataQueryResult(productId, productName, found, variants);
    }

    public String productId() {
        return productId;
    }

    public String productName() {
        return productName;
    }

    public boolean found() {
        return found;
    }

    public List<VariantView> variants() {
        return variants;
    }

    public byte[] encode() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(productId);
            out.writeUTF(productName);
            out.writeBoolean(found);
            out.writeInt(variants.size());
            for (VariantView variant : variants) {
                out.writeUTF(variant.variantId());
                out.writeUTF(variant.variantName());
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding reference-data query result", e);
        }
    }

    public static Optional<ReferenceDataQueryResult> decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return Optional.empty();
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            String productId = in.readUTF();
            String productName = in.readUTF();
            boolean found = in.readBoolean();
            int size = in.readInt();
            List<VariantView> variants = new ArrayList<>(Math.max(0, size));
            for (int i = 0; i < size; i++) {
                variants.add(new VariantView(in.readUTF(), in.readUTF()));
            }
            return Optional.of(new ReferenceDataQueryResult(productId, productName, found, variants));
        } catch (IOException e) {
            return Optional.empty();
        }
    }
}
