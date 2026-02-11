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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.gautelis.raft.proto.Envelope;

public class ProtobufLiteEncoder extends MessageToByteEncoder<Envelope> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Envelope msg, ByteBuf out) {
        byte[] bytes = msg.toByteArray();
        writeRawVarint32(out, bytes.length);
        out.writeBytes(bytes);
    }

    private static void writeRawVarint32(ByteBuf out, int value) {
        while ((value & ~0x7f) != 0) {
            out.writeByte((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value);
    }
}
