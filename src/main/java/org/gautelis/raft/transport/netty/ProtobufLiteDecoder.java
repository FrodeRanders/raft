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
package org.gautelis.raft.transport.netty;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.gautelis.raft.proto.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProtobufLiteDecoder extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(ProtobufLiteDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        int length = readRawVarint32(in);
        if (length == -1) {
            return;
        }

        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return;
        }

        byte[] bytes = new byte[length];
        in.readBytes(bytes);

        try {
            out.add(Envelope.parseFrom(bytes));
        } catch (InvalidProtocolBufferException e) {
            log.warn("Ignoring malformed protobuf payload ({} bytes)", length, e);
        }
    }

    private static int readRawVarint32(ByteBuf buffer) {
        if (!buffer.isReadable()) {
            return -1;
        }

        buffer.markReaderIndex();
        byte tmp = buffer.readByte();
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if (!buffer.isReadable()) {
            buffer.resetReaderIndex();
            return -1;
        }
        tmp = buffer.readByte();
        if (tmp >= 0) {
            return result | tmp << 7;
        }
        result |= (tmp & 0x7f) << 7;
        if (!buffer.isReadable()) {
            buffer.resetReaderIndex();
            return -1;
        }
        tmp = buffer.readByte();
        if (tmp >= 0) {
            return result | tmp << 14;
        }
        result |= (tmp & 0x7f) << 14;
        if (!buffer.isReadable()) {
            buffer.resetReaderIndex();
            return -1;
        }
        tmp = buffer.readByte();
        if (tmp >= 0) {
            return result | tmp << 21;
        }
        result |= (tmp & 0x7f) << 21;
        if (!buffer.isReadable()) {
            buffer.resetReaderIndex();
            return -1;
        }
        tmp = buffer.readByte();
        if (tmp >= 0) {
            return result | tmp << 28;
        }

        log.warn("Malformed varint length prefix");
        return -1;
    }
}
