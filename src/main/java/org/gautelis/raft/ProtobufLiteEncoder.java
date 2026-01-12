package org.gautelis.raft;

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
