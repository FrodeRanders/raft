package org.gautelis.raft;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ByteBufToJsonDecoder extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(ByteBufToJsonDecoder.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() == 0) {
            return;
        }

        int readableBytes = in.readableBytes();
        byte[] bytes = new byte[readableBytes];
        in.readBytes(bytes);

        try {
            JsonNode root = objectMapper.readTree(bytes); // UTF-8 is the default
            out.add(root);
        }
        catch (JsonParseException jpe) {
            log.warn("Ignoring malformed JSON: {}", Arrays.toString(bytes));
        }
    }
}