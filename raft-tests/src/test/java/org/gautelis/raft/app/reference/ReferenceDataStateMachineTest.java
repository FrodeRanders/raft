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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReferenceDataStateMachineTest {
    @Test
    void appliesStructuredProductAndVariantCommands() {
        ReferenceDataStateMachine stateMachine = new ReferenceDataStateMachine();

        stateMachine.apply(1L, ReferenceDataCommand.upsertProduct("p1", "Widget").encode());
        stateMachine.apply(1L, ReferenceDataCommand.upsertVariant("p1", "v1", "Blue").encode());
        stateMachine.apply(1L, ReferenceDataCommand.upsertVariant("p1", "v2", "Red").encode());

        ReferenceDataStateMachine.Product product = stateMachine.getProduct("p1");
        assertNotNull(product);
        assertEquals("Widget", product.productName());
        assertEquals(2, product.variants().size());
    }

    @Test
    void queryReturnsVariantsForProduct() {
        ReferenceDataStateMachine stateMachine = new ReferenceDataStateMachine();
        stateMachine.apply(1L, ReferenceDataCommand.upsertProduct("p1", "Widget").encode());
        stateMachine.apply(1L, ReferenceDataCommand.upsertVariant("p1", "v1", "Blue").encode());

        byte[] payload = stateMachine.query(ReferenceDataQuery.variantsForProduct("p1").encode());
        ReferenceDataQueryResult result = ReferenceDataQueryResult.decode(payload).orElseThrow();

        assertTrue(result.found());
        assertEquals("p1", result.productId());
        assertEquals("Widget", result.productName());
        assertEquals(1, result.variants().size());
        assertEquals("v1", result.variants().getFirst().variantId());
    }

    @Test
    void snapshotRoundtripPreservesNestedVariants() {
        ReferenceDataStateMachine stateMachine = new ReferenceDataStateMachine();
        stateMachine.apply(1L, ReferenceDataCommand.upsertProduct("p1", "Widget").encode());
        stateMachine.apply(1L, ReferenceDataCommand.upsertVariant("p1", "v1", "Blue").encode());
        stateMachine.apply(1L, ReferenceDataCommand.upsertVariant("p1", "v2", "Red").encode());

        byte[] snapshot = stateMachine.snapshot();

        ReferenceDataStateMachine restored = new ReferenceDataStateMachine();
        restored.restore(snapshot);

        ReferenceDataQueryResult result = ReferenceDataQueryResult.decode(
                restored.query(ReferenceDataQuery.variantsForProduct("p1").encode())
        ).orElseThrow();

        assertTrue(result.found());
        assertEquals(2, result.variants().size());
        assertFalse(result.variants().isEmpty());
    }
}
