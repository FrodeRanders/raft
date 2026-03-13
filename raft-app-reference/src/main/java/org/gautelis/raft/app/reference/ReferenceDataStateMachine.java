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

import org.gautelis.raft.statemachine.QueryableStateMachine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple aggregate-oriented reference-data state machine with products and nested variants.
 */
public class ReferenceDataStateMachine implements QueryableStateMachine {
    public record Variant(String variantId, String variantName) {
    }

    public record Product(String productId, String productName, Map<String, Variant> variants) {
        public Product {
            variants = Collections.unmodifiableMap(new LinkedHashMap<>(variants));
        }
    }

    private final Map<String, ProductRecord> products = new LinkedHashMap<>();

    @Override
    public void apply(long term, byte[] command) {
        var parsed = ReferenceDataCommand.decode(command);
        if (parsed.isEmpty()) {
            return;
        }
        ReferenceDataCommand decoded = parsed.get();
        switch (decoded.type()) {
            case UPSERT_PRODUCT -> {
                if (decoded.productId().isBlank()) {
                    return;
                }
                ProductRecord existing = products.get(decoded.productId());
                if (existing == null) {
                    products.put(decoded.productId(), new ProductRecord(decoded.productId(), decoded.productName()));
                } else {
                    existing.productName = decoded.productName();
                }
            }
            case REMOVE_PRODUCT -> products.remove(decoded.productId());
            case UPSERT_VARIANT -> {
                if (decoded.productId().isBlank() || decoded.variantId().isBlank()) {
                    return;
                }
                ProductRecord product = products.computeIfAbsent(decoded.productId(), id -> new ProductRecord(id, ""));
                product.variants.put(decoded.variantId(), new Variant(decoded.variantId(), decoded.variantName()));
            }
            case REMOVE_VARIANT -> {
                ProductRecord product = products.get(decoded.productId());
                if (product != null) {
                    product.variants.remove(decoded.variantId());
                }
            }
        }
    }

    @Override
    public byte[] snapshot() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(products.size());
            for (ProductRecord product : products.values()) {
                out.writeUTF(product.productId);
                out.writeUTF(product.productName);
                out.writeInt(product.variants.size());
                for (Variant variant : product.variants.values()) {
                    out.writeUTF(variant.variantId());
                    out.writeUTF(variant.variantName());
                }
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed creating reference-data snapshot", e);
        }
    }

    @Override
    public void restore(byte[] snapshotData) {
        products.clear();
        if (snapshotData == null || snapshotData.length == 0) {
            return;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshotData))) {
            int productCount = in.readInt();
            for (int i = 0; i < productCount; i++) {
                ProductRecord product = new ProductRecord(in.readUTF(), in.readUTF());
                int variantCount = in.readInt();
                for (int j = 0; j < variantCount; j++) {
                    Variant variant = new Variant(in.readUTF(), in.readUTF());
                    product.variants.put(variant.variantId(), variant);
                }
                products.put(product.productId, product);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed restoring reference-data snapshot", e);
        }
    }

    @Override
    public byte[] query(byte[] request) {
        var parsed = ReferenceDataQuery.decode(request);
        if (parsed.isEmpty()) {
            return new byte[0];
        }
        ReferenceDataQuery decoded = parsed.get();
        if (decoded.type() != ReferenceDataQuery.Type.VARIANTS_FOR_PRODUCT) {
            return new byte[0];
        }
        ProductRecord product = products.get(decoded.productId());
        if (product == null) {
            return ReferenceDataQueryResult.variantsForProduct(decoded.productId(), "", false, List.of()).encode();
        }
        List<ReferenceDataQueryResult.VariantView> variants = new ArrayList<>();
        for (Variant variant : product.variants.values()) {
            variants.add(new ReferenceDataQueryResult.VariantView(variant.variantId(), variant.variantName()));
        }
        return ReferenceDataQueryResult.variantsForProduct(product.productId, product.productName, true, variants).encode();
    }

    public Product getProduct(String productId) {
        ProductRecord product = products.get(productId);
        if (product == null) {
            return null;
        }
        return new Product(product.productId, product.productName, product.variants);
    }

    private static final class ProductRecord {
        private final String productId;
        private String productName;
        private final LinkedHashMap<String, Variant> variants = new LinkedHashMap<>();

        private ProductRecord(String productId, String productName) {
            this.productId = productId;
            this.productName = productName == null ? "" : productName;
        }
    }
}
