package nl.vu.cs.ajira.storage.container;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.data.types.bytearray.BDataOutput;
import nl.vu.cs.ajira.data.types.bytearray.ByteArray;
import nl.vu.cs.ajira.data.types.bytearray.CBDataInput;
import nl.vu.cs.ajira.data.types.bytearray.CBDataOutput;
import nl.vu.cs.ajira.data.types.bytearray.FDataInput;
import nl.vu.cs.ajira.storage.Container;
import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.RawComparator;
import nl.vu.cs.ajira.storage.TupleComparator;
import nl.vu.cs.ajira.storage.Writable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WritableContainer<K extends Writable> extends Writable implements
		Container<K> {

	static final Logger log = LoggerFactory.getLogger(WritableContainer.class);

	protected ByteArray cb = new ByteArray();
	protected int nElements = 0;
	private int pointerLastElement;
	private int lengthLastElement;
	private final int maxSize;

	// These two fields are not supposed to be active at the same time
	protected boolean enableFieldDelimitors = false;
	protected boolean enableRandomAccess = false;
	protected int[] indexElements = null;

	protected BDataOutput output = null;
	protected BDataInput input = null;

	public int compare(byte[] buffer, int start) {
		int len;
		if (this.cb.start < this.cb.end)
			len = this.cb.end - this.cb.start;
		else
			len = this.cb.end + buffer.length - this.cb.start;

		return RawComparator.compareBytes(this.cb.buffer, this.cb.start, len,
				buffer, start + 14, len);
	}

	@Override
	public int bytesToStore() {
		if (nElements == 0) {
			return 10;
		} else {
			int size;
			if (cb.end >= cb.start) {
				size = 14 + cb.end - cb.start;
			} else {
				size = 14 + cb.end + cb.buffer.length - cb.start;
			}
			if (enableRandomAccess)
				size += nElements + 1;
			return size;
		}
	}

	public long inmemory_size() {
		return cb.buffer.length;
	}

	public int getRawElementsSize() {
		if (cb.end >= cb.start) {
			return cb.end - cb.start;
		} else {
			return cb.end + cb.buffer.length - cb.start;
		}
	}

	public int remainingCapacity(int maxSize) {
		int currentSize;
		if (cb.end >= cb.start) {
			currentSize = cb.end - cb.start;
		} else {
			currentSize = cb.buffer.length - cb.start + cb.end;
		}
                // Since we assume that the buffer is empty if cb.end == cb.start,
                // we cannot fill the buffer completely, hence -1. --Ceriel
		return maxSize - currentSize - 1;
	}

	private boolean grow(int sz) {
		if (remainingCapacity(maxSize) < sz) {
			if (log.isDebugEnabled()) {
				log.debug("Grow() fails! maxSize = " + maxSize + ", sz = " + sz
						+ ", remaining capacity = "
						+ remainingCapacity(maxSize));
			}
			return false;
		}
		int currentSize;
		if (cb.end >= cb.start) {
			currentSize = cb.end - cb.start;
		} else {
			currentSize = cb.buffer.length - cb.start + cb.end;
		}
		int len = cb.buffer.length;
		int remaining = len - currentSize;

		while (remaining < sz) {
			len <<= 1;
			remaining = len - currentSize;
		}
		if (len != cb.buffer.length) {
			cb.growBuffer(len);
		}
		return true;
	}

	public WritableContainer(Boolean circular, Boolean enableFieldMarks,
			Boolean enableRandomAccess, int size) {

		maxSize = size;
		if (maxSize >= 256 * 1024) {
			while (size >= 256 * 1024) {
				size >>= 1;
			}
		}
		cb.setBuffer(new byte[size]);
		if (circular) {
			input = new CBDataInput(cb);
			output = new CBDataOutput(cb);
		} else {
			input = new BDataInput(cb);
			output = new BDataOutput(cb);
		}

		this.enableFieldDelimitors = enableFieldMarks;
		this.enableRandomAccess = enableRandomAccess;
		if (enableFieldMarks && enableRandomAccess) {
			throw new Error(
					"enableFieldMarks and enableRandomAccess should not be enabled simultaneously.");
		}
		if (enableRandomAccess) {
			indexElements = new int[4];
			indexElements[0] = cb.start;
		}
		pointerLastElement = -1;
	}

	public WritableContainer(Boolean enableFieldMarks,
			Boolean enableRandomAccess, Integer size) {
		this(new Boolean(true), enableFieldMarks, enableRandomAccess, size);
	}

	public WritableContainer(Integer size) {
		this(true, false, false, size);
	}

	protected void increaseIndexSize(int nEl) {
		int newSize = 2 * indexElements.length;
		while (nEl >= newSize) {
			newSize = 2 * newSize;
		}
		int[] newIndex = new int[newSize];
		System.arraycopy(indexElements, 0, newIndex, 0, indexElements.length);
		indexElements = newIndex;
	}

	@Override
	public void readFrom(java.io.DataInput input) throws IOException {
		nElements = (int) input.readLong();
		enableRandomAccess = input.readBoolean();
		enableFieldDelimitors = input.readBoolean();
		cb.start = 0;
		cb.end = 0;
		if (nElements > 0) {
			int len = input.readInt();
			grow(len);
			cb.end = len;
			input.readFully(cb.buffer, 0, cb.end);

			if (enableRandomAccess) {

				if (indexElements == null) {
					indexElements = new int[4];
				}
				if (nElements >= indexElements.length) {
					increaseIndexSize(nElements + 1);
				}
				for (int i = 0; i < nElements; ++i) {
					indexElements[i] = input.readUnsignedByte();
				}
				indexElements[nElements] = cb.end;
				input.readByte(); // Read the last byte that mark the end of the
				// thing
			}
		}
		pointerLastElement = -1;
	}

	@Override
	public void writeTo(java.io.DataOutput output) throws IOException {
		output.writeLong(nElements);
		output.writeBoolean(enableRandomAccess);
		output.writeBoolean(enableFieldDelimitors);
		if (nElements > 0) {
			int size = 0;
			if (cb.end > cb.start) {
				size = cb.end - cb.start;
				output.writeInt(size);
				output.write(cb.buffer, cb.start, cb.end - cb.start);
			} else {
				size = cb.end + cb.buffer.length - cb.start;
				output.writeInt(size);
				output.write(cb.buffer, cb.start, cb.buffer.length - cb.start);
				output.write(cb.buffer, 0, cb.end);
			}

			if (enableRandomAccess) {
				for (int i = 0; i < nElements; ++i)
					// FIXME: cannot get longer than 255 bytes
					output.write(indexElements[i]);
				output.write(size);
			}
		}
	}

	public void clear() {
		nElements = cb.start = cb.end = 0;
		pointerLastElement = -1;
		if (indexElements != null) {
			indexElements[0] = 0;
		}
		int size = maxSize;
		while (size >= 256 * 1024) {
			size >>= 1;
		}
		if (cb.buffer.length > size) {
			cb.setBuffer(new byte[size]);
		}
	}

	@Override
	public boolean add(K element) {
		
			int len = element.bytesToStore();

			if (!grow(len + 5)) {
				return false;
			}

			// Write the length of the element in the buffer
			if (enableFieldDelimitors) {
				output.writeInt(len);
				pointerLastElement = cb.end;
				lengthLastElement = len;
			}

			try {
				element.writeTo(output);
			} catch (IOException e) {
				log.error("Got exception that should not happen", e);
			}

			nElements++;
			if (enableRandomAccess) {
				if (nElements >= indexElements.length) {
					increaseIndexSize(nElements + 1);
				}
				indexElements[nElements] = cb.end;
			}

		return true;
	}

	@Override
	public boolean addAll(WritableContainer<K> buffer) {

		if (enableRandomAccess) {
			throw new UnsupportedOperationException(
					"addAll is not supported if randomAccess is active");
		}

		if (enableFieldDelimitors != buffer.enableFieldDelimitors) {
			throw new UnsupportedOperationException(
					"addAll works only if the two buffers share the parameter FieldDelimiters");
		}

		int necessarySpace = buffer.cb.buffer.length
				- buffer.remainingCapacity(buffer.cb.buffer.length);
		if (!grow(necessarySpace)) {
			return false;
		}

		if (buffer.cb.end > buffer.cb.start) {
			output.write(buffer.cb.buffer, buffer.cb.start, buffer.cb.end
					- buffer.cb.start);
		} else if (buffer.cb.end < buffer.cb.start) {
			output.write(buffer.cb.buffer, buffer.cb.start,
					buffer.cb.buffer.length - buffer.cb.start);
			output.write(buffer.cb.buffer, 0, buffer.cb.end);
		}

		nElements += buffer.getNElements();

		if (buffer.pointerLastElement >= 0) {
			lengthLastElement = buffer.lengthLastElement;
			pointerLastElement = cb.end - lengthLastElement;
			if (pointerLastElement < 0) {
				pointerLastElement += cb.buffer.length;
			}
		} else {
			pointerLastElement = -1;
		}
		return true;
	}

	@Override
	public boolean remove(K element) {
		if (cb.start == cb.end)
			return false;

		if (enableFieldDelimitors) // Skip the length of the element
			input.readInt();

		try {
			element.readFrom(input);
		} catch (IOException e) {
			log.error("Got exception that should not happen", e);
		}

		--nElements;
		return true;
	}

	public byte[] removeRaw(byte[] value) {
		if (!enableFieldDelimitors)
			throw new UnsupportedOperationException("Method not supported");

		if (cb.start == cb.end)
			return null;

		int length = input.readInt();
		byte[] v;
		if (value != null && value.length == length) {
			v = value;
		} else {
			v = new byte[length];
		}
		input.readFully(v);

		--nElements;
		return v;
	}

	public boolean get(K element) {
		if (cb.start == cb.end)
			return false;

		int originalStart = cb.start;

		if (enableFieldDelimitors) // Skip the length of the element
			input.readInt();
		try {
			element.readFrom(input);
		} catch (IOException e) {
			log.error("Got exception that should not happen");
		}

		cb.start = originalStart;
		return true;
	}

	@Override
	public int getNElements() {
		return nElements;
	}

	@Override
	public boolean get(K element, int index) {
		if (!enableRandomAccess) {
			throw new UnsupportedOperationException(
					"Method not supported if random access is disabled");
		}

		if (index >= nElements) {
			return false;
		}

		int originalStart = cb.start;
		cb.start = indexElements[index];
		try {
			element.readFrom(input);
		} catch (IOException e) {
			log.error("This exception should not happen", e);
		}
		cb.start = originalStart;
		return true;
	}

	public void copyTo(WritableContainer<?> buffer) {
		buffer.enableFieldDelimitors = enableFieldDelimitors;
		buffer.enableRandomAccess = enableRandomAccess;
		buffer.nElements = nElements;
		buffer.cb.start = cb.start;
		buffer.cb.end = cb.end;
		buffer.lengthLastElement = lengthLastElement;
		buffer.pointerLastElement = pointerLastElement;
		if (cb.end > cb.start) {
			System.arraycopy(this.cb.buffer, cb.start, buffer.cb.buffer,
					cb.start, cb.end - cb.start);
		} else if (cb.start > cb.end) {
			System.arraycopy(this.cb.buffer, cb.start, buffer.cb.buffer,
					cb.start, this.cb.buffer.length - cb.start);
			System.arraycopy(this.cb.buffer, 0, buffer.cb.buffer, 0, cb.end);
		}

		if (enableRandomAccess) {
			if (buffer.indexElements == null
					|| buffer.indexElements.length < indexElements.length) {
				buffer.indexElements = new int[indexElements.length];
			}
			System.arraycopy(indexElements, 0, buffer.indexElements, 0,
					nElements + 1);
		}
	}

	public int getHash(int maxBytes) {
		int hash = 0;
		if (cb.end > cb.start) {
			for (int i = cb.start; maxBytes > 0 && i < cb.end; i++, maxBytes--) {
				hash = 31 * hash + (cb.buffer[i] & 0xff);
			}
		} else {
			for (int i = cb.start; maxBytes > 0 && i < cb.buffer.length; i++, maxBytes--) {
				hash = 31 * hash + (cb.buffer[i] & 0xff);
			}
			for (int i = 0; maxBytes > 0 && i < cb.end; i++, maxBytes--) {
				hash = 31 * hash + (cb.buffer[i] & 0xff);
			}
		}
		return hash;
	}

	public int getHash(int index, int maxBytes) throws Exception {
		if (!enableRandomAccess) {
			throw new Exception(
					"Method not supported if random access is disabled");
		}
		int s = indexElements[index];
		int hash = 0;
		if (cb.end > s) {
			for (int i = s; maxBytes > 0 && i < cb.end; i++, maxBytes--) {
				hash = 31 * hash + (cb.buffer[i] & 0xff);
			}
		} else {
			for (int i = s; maxBytes > 0 && i < cb.buffer.length; i++, maxBytes--) {
				hash = 31 * hash + (cb.buffer[i] & 0xff);
			}
			for (int i = 0; maxBytes > 0 && i < cb.end; i++, maxBytes--) {
				hash = 31 * hash + (cb.buffer[i] & 0xff);
			}
		}
		return hash;
	}

	public void moveTo(WritableContainer<?> buffer) {
		copyTo(buffer);
		clear();
	}

	public void removeLast() throws Exception {
		if (!enableRandomAccess) {
			throw new Exception(
					"Method not supported if random access is disabled");
		}

		cb.end = indexElements[--nElements];
	}

	// long rawTime = 0;
	// void addTime(long time) {
	// rawTime += time;
	// }

	private void copyRegion(int index, int len) {
		// Appends a copy of a region. This is a bit complicated because both
		// the source and the target (cb.end) may wrap. Solution: if the source
		// wraps, split it into two parts.
		if (len == 0) {
			return;
		}
		if (index + len <= cb.buffer.length) {
			// source does not wrap.
			if (cb.end + len <= cb.buffer.length) {
				// dest does not wrap.
				System.arraycopy(cb.buffer, index, cb.buffer, cb.end, len);
				cb.end += len;
			} else {
				// dest wraps.
				int len1 = cb.buffer.length - cb.end;
				System.arraycopy(cb.buffer, index, cb.buffer, cb.end, len1);
				System.arraycopy(cb.buffer, index + len1, cb.buffer, 0, len
						- len1);
				cb.end = len - len1;
			}
		} else {
			// source wraps
			int len1 = cb.buffer.length - index;
			copyRegion(index, len1);
			copyRegion(0, len - len1);
		}
	}

	public void sort(final RawComparator<K> c, Factory<WritableContainer<K>> fb) {

		if (!enableFieldDelimitors) {
			throw new Error(
					"Sorting doesn't work if the flag enableFieldDelimiters is set to false");
		}

		// 1) Populate
		long time = System.currentTimeMillis();

		int l = 0;
		final int[] coordinates = new int[(nElements * 2)];
		Integer[] indexes = new Integer[nElements];

		long size = getRawElementsSize();

		int i = 0;
		while (nElements > 0) {
			int length = input.readInt();
			/*
			 * if (log.isDebugEnabled()) { if (length > 256) {
			 * log.debug("OOPS, length = " + length); } }
			 */
			coordinates[l] = cb.start;
			coordinates[l + 1] = length;
			cb.start = (coordinates[l] + coordinates[l + 1]) % cb.buffer.length;
			nElements--;
			indexes[i] = i * 2;
			i++;
			l += 2;
		}
		log.debug("Time populate (\t" + indexes.length + "\t):\t"
				+ (System.currentTimeMillis() - time) + "\tT:"
				+ Thread.currentThread().getId());

		// 2) Sort
		time = System.currentTimeMillis();
		((TupleComparator) c).timeConverting = 0;
		Arrays.sort(indexes, new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				int response = c.compare(cb.buffer, coordinates[o1],
						coordinates[o1 + 1], cb.buffer, coordinates[o2],
						coordinates[o2 + 1]);
				return response;
			}
		});

		log.debug("Time sorting (\t" + indexes.length + "\t):\t"
				+ (System.currentTimeMillis() - time) + "\tT:"
				+ Thread.currentThread().getId() + "-"
				+ ((TupleComparator) c).timeConverting);

		// 3) Repopulate
		time = System.currentTimeMillis();
		if (size < cb.buffer.length / 2) {
			for (int index : indexes) {
				output.writeInt(coordinates[index + 1]);
				copyRegion(coordinates[index], coordinates[index + 1]);
				nElements++;
			}
		} else { // It's too big. Must use another array
			WritableContainer<K> newArray = fb.get();
			newArray.clear();
			newArray.grow((int) size);
			for (int index : indexes) {
				newArray.output.writeInt(coordinates[index + 1]);
				if (coordinates[index] + coordinates[index + 1] > cb.buffer.length) {
					// Note, newArray cannot wrap, since it is new ...
					int len1 = cb.buffer.length - coordinates[index];
					System.arraycopy(cb.buffer, coordinates[index],
							newArray.cb.buffer, newArray.cb.end, len1);
					System.arraycopy(cb.buffer, 0, newArray.cb.buffer,
							newArray.cb.end + len1, coordinates[index + 1]
									- len1);
				} else {
					System.arraycopy(cb.buffer, coordinates[index],
							newArray.cb.buffer, newArray.cb.end,
							coordinates[index + 1]);
				}
				newArray.cb.end += coordinates[index + 1];
				newArray.nElements++;
			}
			newArray.copyTo(this);
			// fb.release(newArray);

		}

		/*
		 * // Consystency check if (log.isDebugEnabled()) { int savedStart =
		 * cb.start; for (int j = 0; j < nElements; j++) { int length =
		 * input.readInt(); if (length != coordinates[indexes[j]+1]) {
		 * log.debug("OOPS: consistency error: length = " + length +
		 * ", should be " + coordinates[indexes[j]+1]); } cb.start = (cb.start +
		 * length) % cb.buffer.length; } cb.start = savedStart; }
		 */

		log.debug("Time repopulate (\t" + indexes.length + "\t):\t"
				+ (System.currentTimeMillis() - time) + "\tT:"
				+ Thread.currentThread().getId());

		int lastIndex = indexes[indexes.length - 1];
		lengthLastElement = coordinates[lastIndex + 1];
		pointerLastElement = cb.end - lengthLastElement;
		if (pointerLastElement < 0) {
			pointerLastElement += cb.buffer.length;
		}
	}

	public void writeElementsTo(DataOutput cacheOutputStream)
			throws IOException {
		/*
		 * // Consistency check. if (log.isDebugEnabled()) { int savedStart =
		 * cb.start; for (int j = 0; j < nElements; j++) { int length =
		 * input.readInt(); if (length > 256) { log.debug("OOPS: length = " +
		 * length); } cb.start = (cb.start + length) % cb.buffer.length; } if
		 * (cb.end != cb.start) {
		 * log.debug("Something wrong with this container! cb.end = " + cb.end +
		 * ", but found end " + cb.start); } cb.start = savedStart; }
		 */

		if (cb.end > cb.start) {
			cacheOutputStream
					.write(this.cb.buffer, cb.start, cb.end - cb.start);
		} else if (cb.start > cb.end) {
			cacheOutputStream.write(this.cb.buffer, cb.start, cb.buffer.length
					- cb.start);
			cacheOutputStream.write(this.cb.buffer, 0, cb.end);
		}
	}

	public boolean addRaw(byte[] key) {

		if (!grow(key.length + 4)) {
			return false;
		}

		if (enableFieldDelimitors) {
			output.writeInt(key.length);
			lengthLastElement = key.length;
			pointerLastElement = cb.end;
		}

		output.write(key);

		nElements++;
		if (enableRandomAccess) {
			if (nElements >= indexElements.length) {
				increaseIndexSize(nElements + 1);
			}
			indexElements[nElements] = cb.end;
		}

		return true;
	}

	public int compareTo(WritableContainer<K> buffer) {
		// This method works only if the buffers are not circulars
		int len1 = cb.end - cb.start;
		int len2 = buffer.cb.end - buffer.cb.start;
		return RawComparator.compareBytes(cb.buffer, cb.start, len1,
				buffer.cb.buffer, buffer.cb.start, len2);
	}

	public void addRaw(WritableContainer<K> buffer, int i) {
		// This method works only if the buffers are not circulars

		if (!buffer.enableRandomAccess || buffer.enableFieldDelimitors) {
			throw new UnsupportedOperationException("Not supported");
		}

		int start = buffer.indexElements[i];
		int length = buffer.indexElements[i + 1] - start;

		System.arraycopy(buffer.cb.buffer, start, cb.buffer, cb.end, length);
		cb.end += length;
		nElements++;
		if (nElements >= indexElements.length) {
			increaseIndexSize(nElements + 1);
		}
		indexElements[nElements] = cb.end;
	}

	public byte[] returnLastElement() {
		if (pointerLastElement < 0) {
			// Not available.
			return null;
		}
		byte[] value = new byte[lengthLastElement];
		int previousStart = cb.start;
		cb.start = pointerLastElement;
		input.readFully(value);
		cb.start = previousStart;
		return value;
	}

	public boolean addAll(FDataInput originalStream, byte[] lastEl,
			long nElements, long size) throws IOException {
		if (enableRandomAccess) {
			throw new UnsupportedOperationException("Not supported!");
		}

		if (!grow((int) size)) {
			return false;
		}

		if (size > cb.buffer.length - cb.end) {
			originalStream.readFully(cb.buffer, cb.end, cb.buffer.length
					- cb.end);
			cb.end = (int) (size - cb.buffer.length + cb.end);
			originalStream.readFully(cb.buffer, 0, cb.end);

		} else {
			originalStream.readFully(cb.buffer, cb.end, (int) size);
			cb.end += size;
		}

		this.nElements += nElements;
		if (enableFieldDelimitors && nElements > 0 && lastEl != null) {
			lengthLastElement = lastEl.length;
			pointerLastElement = cb.end - lengthLastElement;
			if (pointerLastElement < 0) {
				pointerLastElement += cb.buffer.length;
			}
		} else {
			pointerLastElement = -1;
		}

		return true;
	}
}
