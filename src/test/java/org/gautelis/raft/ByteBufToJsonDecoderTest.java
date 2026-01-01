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
