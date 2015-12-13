package it.unitn.ds.net;

import io.netty.buffer.ByteBuf;

/**
 * RELOAD link layer message
 */
public abstract class LinkMessage {

	public static enum Type {
		DATA(0x1), ACK(0x2);

		public final byte code;

		Type(int code) {
			this.code = (byte) code;
		}

		public static Type valueOf(byte code) {
			for (Type t : Type.values())
				if (t.code == code)
					return t;
			return null;
		}
	};

	// Maximum sequence number
	public static final long SEQ_MAX_VALUE = 0xffffffffl;

	public abstract long getSequence();

	public abstract Type getType();

	/**
	 * Link level data message
	 */
	public static class FramedData extends LinkMessage {

		protected long sequence;
		protected ByteBuf payload;

		public FramedData(long sequence, ByteBuf payload) {
			this.sequence = sequence;
			this.payload = payload;
		}

		@Override
		public Type getType() {
			return Type.DATA;
		}

		@Override
		public long getSequence() {
			return sequence;
		}

		public ByteBuf getPayload() {
			return payload;
		}
	}

	/**
	 * Link level acknowledge message
	 */
	public static class FramedAck extends LinkMessage {

		protected final long ack_sequence;

		public FramedAck(long ack_sequence) {
			super();
			this.ack_sequence = ack_sequence;
		}

		@Override
		public Type getType() {
			return Type.ACK;
		}

		@Override
		public long getSequence() {
			return ack_sequence;
		}
	}
}
