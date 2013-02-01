package nl.vu.cs.ajira.data.types.bytearray;

import nl.vu.cs.ajira.storage.RawComparator;

public class ByteArray {
	protected byte[] buffer = null;
	protected int start = 0;
	protected int end = 0;
	protected int maxSize = 0;

	public ByteArray(int size, int maxSize) {
		buffer = new byte[size];
		this.maxSize = maxSize;
	}

	public ByteArray() {
	}

	public int getEnd() {
		return end;
	}

	public byte[] getBuffer() {
		return buffer;
	}

	public int compare(byte[] buffer, int start) {
		int len;
		if (start < end)
			len = end - start;
		else
			len = end + buffer.length - start;

		return RawComparator.compareBytes(buffer, start, len, buffer,
				start + 14, len);
	}

	public long getTotalCapacity() {
		return buffer.length;
	}

	public int getRawSize() {
		if (end >= start) {
			return end - start;
		} else {
			return end + buffer.length - start;
		}
	}

	public int remainingCapacity(int maxSize) {
		int currentSize;
		if (end >= start) {
			currentSize = end - start;
		} else {
			currentSize = buffer.length - start + end;
		}
		// Since we assume that the buffer is empty if cb.end == cb.start,
		// we cannot fill the buffer completely, hence -1. --Ceriel
		return maxSize - currentSize - 1;
	}

	private void growBuffer(int sz) {
		byte[] b = new byte[sz];
		if (end >= start) {
			System.arraycopy(buffer, start, b, start, end - start);
		} else {
			System.arraycopy(buffer, start, b, start, buffer.length - start);
			if (b.length >= buffer.length + end) {
				System.arraycopy(buffer, 0, b, buffer.length, end);
				end += buffer.length;
			} else {
				System.arraycopy(buffer, 0, b, buffer.length, b.length
						- buffer.length);
				end -= (b.length - buffer.length);
				System.arraycopy(buffer, b.length - buffer.length, b, 0, end);
			}
		}
		buffer = b;
	}

	protected boolean grow(int sz) {
		if (remainingCapacity(maxSize) < sz) {
			return false;
		}
		int currentSize;
		if (end >= start) {
			currentSize = end - start;
		} else {
			currentSize = buffer.length - start + end;
		}
		int len = buffer.length;
		int remaining = len - currentSize;

		while (remaining < sz) {
			len <<= 1;
			remaining = len - currentSize;
		}
		if (len != buffer.length) {
			growBuffer(len);
		}
		return true;
	}

	public int getHash(int maxBytes) {
		int hash = 0;
		if (end > start) {
			for (int i = start; maxBytes > 0 && i < end; i++, maxBytes--) {
				hash = 31 * hash + (buffer[i] & 0xff);
			}
		} else {
			for (int i = start; maxBytes > 0 && i < buffer.length; i++, maxBytes--) {
				hash = 31 * hash + (buffer[i] & 0xff);
			}
			for (int i = 0; maxBytes > 0 && i < end; i++, maxBytes--) {
				hash = 31 * hash + (buffer[i] & 0xff);
			}
		}
		return hash;
	}
}
