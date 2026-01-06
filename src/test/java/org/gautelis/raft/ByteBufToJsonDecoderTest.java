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
package org.gautelis.raft;

import com.fasterxml.jackson.databind.JsonNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByteBufToJsonDecoderTest {
    @Test
    void decodesValidJson() {
        EmbeddedChannel channel = new EmbeddedChannel(new ByteBufToJsonDecoder());
        ByteBuf buf = Unpooled.copiedBuffer("{\"a\":1}", StandardCharsets.UTF_8);
        try {
            assertTrue(channel.writeInbound(buf));
            JsonNode out = channel.readInbound();
            assertEquals(1, out.get("a").asInt());
            assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void ignoresMalformedJson() {
        EmbeddedChannel channel = new EmbeddedChannel(new ByteBufToJsonDecoder());
        ByteBuf buf = Unpooled.copiedBuffer("{not-json", StandardCharsets.UTF_8);
        try {
            channel.writeInbound(buf);
            assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
