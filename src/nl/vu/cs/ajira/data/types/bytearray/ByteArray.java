package nl.vu.cs.ajira.data.types.bytearray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// import nl.vu.cs.ajira.mgmt.MemoryManager;
import nl.vu.cs.ajira.storage.RawComparator;
import nl.vu.cs.ajira.utils.Utils;

import org.iq80.snappy.Snappy;

public class ByteArray {
	protected byte[] buffer = null;
	protected int start = 0;
	protected int end = 0;
	protected int maxSize = 0;
	private final int initialSize;

	/**
	 * Creates a new ByteArray and sets the fields of the object.
	 * 
	 * @param size
	 *            the new size of the buffer
	 * @param maxSize
	 *            the maximum size of the buffer
	 */
	public ByteArray(int size, int maxSize) {
		buffer = new byte[size];
		this.maxSize = maxSize;
		initialSize = size;
	}

	public ByteArray(byte[] buf) {
		buffer = buf;
		initialSize = buffer.length;
	}

	public ByteArray() {
		initialSize = 0;
	}

	/**
	 * 
	 * @return the end of the buffer
	 */
	public int getEnd() {
		return end;
	}

	public int getStart() {
		return start;
	}

	public void readFrom(FDataInput in, int sz) throws IOException {
		if (sz > buffer.length - end) {
			in.readFully(buffer, end, buffer.length - end);
			end = sz - buffer.length + end;
			in.readFully(buffer, 0, end);

		} else {
			in.readFully(buffer, end, sz);
			end += sz;
		}
	}

	private void readFrom(byte[] b, int s, int len) {
		int len1 = buffer.length - end;
		if (len > len1) {
			System.arraycopy(b, s, buffer, end, len1);
			end = len - len1;
			System.arraycopy(b, s + len1, buffer, 0, end);
		} else {
			System.arraycopy(b, s, buffer, end, len);
			end += len;
		}
	}

	public void copyRegion(ByteArray in, int offset, int len) {
		if (len == 0) {
			return;
		}
		if (offset + len <= in.buffer.length) {
			readFrom(in.buffer, offset, len);
		} else {
			readFrom(in.buffer, offset, in.buffer.length - offset);
			readFrom(in.buffer, 0, len - (in.buffer.length - offset));
		}
	}

	public void readFrom(ByteArray in) {
		if (in.end >= in.start) {
			readFrom(in.buffer, in.start, in.end - in.start);
		} else {
			readFrom(in.buffer, in.start, in.buffer.length - in.start);
			readFrom(in.buffer, 0, in.end);
		}
	}

	public void readFrom(DataInput input) throws IOException {
		start = 0;
		int len = input.readInt();
		if (!grow(len)) {
			System.err.println("OOPS: could not grow to length " + len
					+ ", maxSize = " + maxSize);
		}
		end = len;
		if (len > 0) {
			input.readFully(buffer, 0, end);
		}
	}

	public void writeRaw(DataOutput out) throws IOException {

		if (end > start) {
			out.write(this.buffer, start, end - start);
		} else if (start > end) {
			out.write(this.buffer, start, buffer.length - start);
			out.write(this.buffer, 0, end);
		}
	}

	public void writeTo(DataOutput out, int offset, int len) throws IOException {
		int e = offset + len;
		if (e <= buffer.length) {
			out.write(buffer, offset, len);
		} else {
			e -= buffer.length;
			out.write(buffer, offset, buffer.length - offset);
			out.write(buffer, 0, e);
		}
	}

	public void writeTo(DataOutput output) throws IOException {
		int size = 0;
		if (end > start) {
			size = end - start;
			output.writeInt(size);
			output.write(buffer, start, end - start);
		} else if (end < start) {
			size = end + buffer.length - start;
			output.writeInt(size);
			output.write(buffer, start, buffer.length - start);
			output.write(buffer, 0, end);
		} else {
			output.writeInt(0);
		}
	}

	/**
	 * Compares the object's buffer with the buffer from the parameters taking
	 * in consideration a staring point.
	 * 
	 * @param buffer
	 *            is the array to whom the object's buffer is compared
	 * @param start
	 *            is the position of the buffer from where the comparison starts
	 * @return 0 in case of equality a number lower than 0 if the object's
	 *         buffer is lower than the parameter a number greater than 0 if the
	 *         object's buffer is greater than the parameter
	 */
	public int compare(byte[] buffer, int start) {
		int len;
		if (start < end)
			len = end - start;
		else
			len = end + buffer.length - start;

		return RawComparator.compareBytes(this.buffer, this.start, len, buffer,
				start, len);
	}

	/**
	 * 
	 * @return the length of the buffer
	 */
	public int getTotalCapacity() {
		return buffer.length;
	}

	/**
	 * If the start is greater than the end the available space is the one from
	 * start until the end of the of the array and the one from the beginning of
	 * the array until the end. If the start is lower than the end the available
	 * space is the one between the start and the end.
	 * 
	 * @return the available space from the buffer
	 */
	public int getRawSize() {
		if (end >= start) {
			return end - start;
		} else {
			return end + buffer.length - start;
		}
	}

	/**
	 * 
	 * @param maxSize
	 *            is the maximum size of the buffer that is considered
	 * @return the remaining capacity of the buffer considering the maximum size
	 *         of the buffer to be maxSize
	 */
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

	/**
	 * The method increases the size of the buffer to sz.
	 * 
	 * @param sz
	 *            is the new length of the buffer.
	 */
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

	/**
	 * If the size of the buffer can be increased then it is increased to
	 * smallest power of 2 for which the remaining space is greater than sz.
	 * 
	 * @param sz
	 *            is the wanted size of the buffer
	 * @return true if the buffer size can be increased at sz false if the
	 *         buffer size cannot be increased at sz
	 */
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

		while (remaining <= sz) {
			len <<= 1;
			remaining = len - currentSize;
		}
		if (len > buffer.length) {
			/*
			 * Commented out code below. It breaks the assumption that you can
			 * always copy a WritableContainer into another empty
			 * WritableContainer. int allowedLength =
			 * MemoryManager.getInstance().canAllocate(len); if (allowedLength <
			 * buffer.length * 1.20) { // ?????? --Ceriel return false; }
			 * growBuffer(allowedLength);
			 */
			growBuffer(len);
		}
		return true;
	}

	/**
	 * 
	 * @param maxBytes
	 *            is the number of bytes from the buffer that are taken in
	 *            consideration for the hash value
	 * @return a hash value
	 */
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

	public void clear() {
		start = end = 0;
		// if (buffer.length > initialSize) {
		// buffer = new byte[initialSize];
		// }
	}

	public int compress(byte[] value, int offset) {
		int size;
		if (end > start) {
			value[offset] = 0;
			size = Snappy.compress(buffer, start, end - start, value,
					offset + 1);
		} else {
			value[offset] = 1;
			size = Snappy.compress(buffer, start, buffer.length - start, value,
					offset + 5);
			Utils.encodeInt(value, offset + 1, size);
			size += 4 + Snappy.compress(buffer, 0, end, value, offset + 5
					+ size);
		}
		return size + 1;
	}

	public void decompress(byte[] value, int offset, int len) {
		if (value[offset] == 0) {
			grow(Snappy.getUncompressedLength(value, offset + 1));
			end = Snappy.uncompress(value, offset + 1, len - 1, buffer, 0);
		} else {
			int s = Utils.decodeInt(value, offset + 1);
			// Calculate uncompress size
			offset += 5;
			len -= 5;
			int un_s = Snappy.getUncompressedLength(value, offset);
			un_s += Snappy.getUncompressedLength(value, offset + s);
			grow(un_s);

			end = Snappy.uncompress(value, offset, s, buffer, 0);
			end += Snappy.uncompress(value, offset + s, len - s, buffer, end);
		}
	}
}
