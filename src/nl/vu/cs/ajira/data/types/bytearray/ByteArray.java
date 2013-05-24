package nl.vu.cs.ajira.data.types.bytearray;

// import nl.vu.cs.ajira.mgmt.MemoryManager;
import nl.vu.cs.ajira.storage.RawComparator;

public class ByteArray {
	protected byte[] buffer = null;
	protected int start = 0;
	protected int end = 0;
	protected int maxSize = 0;

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
	}

	/**
	 * Creates an empty ByteArray.
	 */
	public ByteArray() {
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

	/**
	 * 
	 * @return the buffer of the object
	 */
	public byte[] getBuffer() {
		return buffer;
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
	public long getTotalCapacity() {
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
			 * Commented out code below. It breaks the assumption that you can always
			 * copy a WritableContainer into another empty WritableContainer.
			int allowedLength = MemoryManager.getInstance().canAllocate(len);
			if (allowedLength < buffer.length * 1.20) { // ?????? --Ceriel
				return false;
			}
			growBuffer(allowedLength);
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
}