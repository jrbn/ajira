package nl.vu.cs.ajira.storage.containers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import nl.vu.cs.ajira.buckets.TupleComparator;
import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.data.types.bytearray.BDataOutput;
import nl.vu.cs.ajira.data.types.bytearray.ByteArray;
import nl.vu.cs.ajira.data.types.bytearray.CBDataInput;
import nl.vu.cs.ajira.data.types.bytearray.CBDataOutput;
import nl.vu.cs.ajira.data.types.bytearray.FDataInput;
import nl.vu.cs.ajira.storage.Container;
import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.RawComparator;
import nl.vu.cs.ajira.storage.Writable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableContainer<K extends Writable> extends ByteArray implements
		Writable, Container<K> {

	static final Logger log = LoggerFactory.getLogger(WritableContainer.class);

	protected int nElements = 0;
	private int pointerLastElement; // <- not necessary in the new version.
									// Should be removed
	private int lengthLastElement;
	protected BDataOutput output;
	protected BDataInput input;

	protected boolean enableFieldDelimitors = false;

	/**
	 * Creates a new object.
	 * 
	 * @param circular 
	 * 		Influences the class used for the input and output fields.
	 * @param enableFieldMarks
	 * 		Is the new value of enableFieldDelimitors.
	 * @param maxSize
	 * 		The maximum size of the ByteArray's buffer. 
	 */
	public WritableContainer(Boolean circular, Boolean enableFieldMarks,
			int maxSize) {
		super(256 * 1024, maxSize);

		if (circular) {
			input = new CBDataInput(this);
			output = new CBDataOutput(this, true);
		} else {
			input = new BDataInput(this);
			output = new BDataOutput(this, true);
		}

		this.enableFieldDelimitors = enableFieldMarks;
		pointerLastElement = -1;
	}

	/**
	 * Constructs a new object and uses CBDataInput 
	 * class for the input and output fields.
	 * 
	 * @param enableFieldMarks
	 * 		Is the new value of enableFieldDelimitors.
	 * @param size
	 * 		The maximum size of the ByteArray's buffer.
	 */
	public WritableContainer(Boolean enableFieldMarks, Integer size) {
		this(new Boolean(true), enableFieldMarks, size);
	}

	/**
	 * Constructs a new object. Uses CBDataInput 
	 * class for the input and output fields and
	 * set the enableFieldDelimitors to false.
	 * 
	 * @param size
	 * 		The maximum size of the ByteArray's buffer.
	 */
	public WritableContainer(Integer size) {
		this(true, false, size);
	}

	/**
	 * Reads from the input the number of elements,
	 * if it should enable delimiters and the elements
	 * of the buffer.
	 */
	@Override
	public void readFrom(DataInput input) throws IOException {
		nElements = (int) input.readLong();
		enableFieldDelimitors = input.readBoolean();
		start = 0;
		end = 0;
		if (nElements > 0) {
			int len = input.readInt();
			grow(len);
			end = len;
			input.readFully(buffer, 0, end);
		}
		pointerLastElement = -1;
	}

	/**
	 * Writes in the output the number of elements,
	 * if the delimiters are enabled and the elments
	 * of the buffer.
	 */
	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeLong(nElements);
		output.writeBoolean(enableFieldDelimitors);
		if (nElements > 0) {
			int size = 0;
			if (end > start) {
				size = end - start;
				output.writeInt(size);
				output.write(buffer, start, end - start);
			} else {
				size = end + buffer.length - start;
				output.writeInt(size);
				output.write(buffer, start, buffer.length - start);
				output.write(buffer, 0, end);
			}
		}
	}

	public void clear() {
		nElements = start = end = 0;
		pointerLastElement = -1;

		if (maxSize > 256 * 1024 && buffer.length > 256 * 1024) {
			buffer = new byte[256 * 1024];
		}
	}

	@Override
	public boolean add(K element) {
		int lastPos = end;
		try {
			// Write the length of the element in the buffer
			if (enableFieldDelimitors) {

				// Move four bytes forward
				int tmpPointerLength = end;
				output.writeInt(0);

				int tmpPointerLastElement = end;
				element.writeTo(output);
				int length;
				if (end >= tmpPointerLastElement) {
					length = end - tmpPointerLastElement;
				} else {
					length = end + buffer.length - tmpPointerLastElement;
				}

				// Rewrite the length
				int currentEnd = end;
				end = tmpPointerLength;
				output.writeInt(length);
				end = currentEnd;

				lengthLastElement = length;
				pointerLastElement = tmpPointerLastElement;
			} else {
				element.writeTo(output);
			}

			nElements++;

			return true;

		} catch (IOException e) {
			end = lastPos;
			return false;
		}
	}

	@Override
	public boolean addAll(WritableContainer<K> buffer) {

		try {

			if (enableFieldDelimitors != buffer.enableFieldDelimitors
					&& nElements > 0) {
				throw new UnsupportedOperationException(
						"addAll works only if the two buffers share the parameter FieldDelimiters");
			}

			int necessarySpace = buffer.buffer.length - 1
					- buffer.remainingCapacity(buffer.buffer.length);	// -1: see code in remainingCapacity --Ceriel
			if (!grow(necessarySpace)) {
				if (log.isDebugEnabled()) {
					log.debug("Grow() fails: necessarySpace = " + necessarySpace);
				}
				return false;
			}

			if (buffer.end > buffer.start) {
				output.write(buffer.buffer, buffer.start, buffer.end
						- buffer.start);
			} else if (buffer.end < buffer.start) {
				output.write(buffer.buffer, buffer.start, buffer.buffer.length
						- buffer.start);
				output.write(buffer.buffer, 0, buffer.end);
			}
			nElements += buffer.getNElements();
			enableFieldDelimitors = buffer.enableFieldDelimitors;

			if (buffer.pointerLastElement >= 0) {
				lengthLastElement = buffer.lengthLastElement;
				pointerLastElement = end - lengthLastElement;
				if (pointerLastElement < 0) {
					pointerLastElement += this.buffer.length;
				}
			} else {
				pointerLastElement = -1;
			}
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public boolean remove(K element) {

		if (start == end)
			return false;

		if (enableFieldDelimitors) // Skip the length of the just read
			// element
			input.skipBytes(4);

		if (log.isDebugEnabled()) {
			if (nElements <= 0) {
				log.error("OOPS: remove on empty container? start = " + start + ", end = " + end);
			}
		}

		try {
			element.readFrom(input);
		} catch (IOException e) {
			log.error("Should not happen", e);
		}
		--nElements;
		return true;
	}

	public byte[] removeRaw(byte[] value) {
		if (!enableFieldDelimitors)
			throw new UnsupportedOperationException("Method not supported");

		if (start == end)
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

	public byte[] removeLastElement() {
		if (pointerLastElement < 0) {
			// Not available.
			return null;
		}
		byte[] value = new byte[lengthLastElement];
		int previousStart = start;
		start = pointerLastElement;
		input.readFully(value);
		start = previousStart;
		return value;
	}

	@Override
	public int getNElements() {
		return nElements;
	}

	public void copyTo(WritableContainer<?> buffer) {
		buffer.enableFieldDelimitors = enableFieldDelimitors;
		buffer.nElements = nElements;
		buffer.start = start;
		buffer.end = end;
		buffer.lengthLastElement = lengthLastElement;
		buffer.pointerLastElement = pointerLastElement;
		if (end > start) {
			System.arraycopy(this.buffer, start, buffer.buffer, start, end
					- start);
		} else if (start > end) {
			System.arraycopy(this.buffer, start, buffer.buffer, start,
					this.buffer.length - start);
			System.arraycopy(this.buffer, 0, buffer.buffer, 0, end);
		}
	}

	public void moveTo(WritableContainer<?> buffer) {
		copyTo(buffer);
		clear();
	}

	private void copyRegion(int index, int len) {
		// Appends a copy of a region. This is a bit complicated because both
		// the source and the target (cb.end) may wrap. Solution: if the source
		// wraps, split it into two parts.
		if (len == 0) {
			return;
		}
		if (index + len <= buffer.length) {
			// source does not wrap.
			if (end + len <= buffer.length) {
				// dest does not wrap.
				System.arraycopy(buffer, index, buffer, end, len);
				end += len;
			} else {
				// dest wraps.
				int len1 = buffer.length - end;
				System.arraycopy(buffer, index, buffer, end, len1);
				System.arraycopy(buffer, index + len1, buffer, 0, len - len1);
				end = len - len1;
			}
		} else {
			// source wraps
			int len1 = buffer.length - index;
			copyRegion(index, len1);
			copyRegion(0, len - len1);
		}
	}

	public void sort(final RawComparator<K> c, Factory<WritableContainer<K>> fb) {

		if (!enableFieldDelimitors) {
			throw new UnsupportedOperationException(
					"Sorting doesn't work if the flag enableFieldDelimiters is set to false");
		}

		// 1) Populate
		long time = System.currentTimeMillis();
		long size = getRawSize();

		int l = 0;
		final int[] coordinates = new int[(nElements * 2)];
		Integer[] indexes = new Integer[nElements];

		int i = 0;
		while (nElements > 0) {
			int length = input.readInt();
			coordinates[l] = start;
			coordinates[l + 1] = length;
			start = (coordinates[l] + coordinates[l + 1]) % buffer.length;
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
				int response = c.compare(buffer, coordinates[o1],
						coordinates[o1 + 1], buffer, coordinates[o2],
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
		if (size < buffer.length / 2) {
			for (int index : indexes) {
				try {
					output.writeInt(coordinates[index + 1]);
				} catch (IOException e) {
					log.error("Internal error, should not happen", e);
				}
				copyRegion(coordinates[index], coordinates[index + 1]);
				nElements++;
			}
		} else { // It's too big. Must use another array
			WritableContainer<K> newArray = fb.get();
			newArray.clear();
			newArray.grow((int) size);
			for (int index : indexes) {
				try {
					newArray.output.writeInt(coordinates[index + 1]);
				} catch (IOException e) {
					log.error("Internal error, should not happen", e);
				}
				if (coordinates[index] + coordinates[index + 1] > buffer.length) {
					// Note, newArray cannot wrap, since it is new ...
					int len1 = buffer.length - coordinates[index];
					System.arraycopy(buffer, coordinates[index],
							newArray.buffer, newArray.end, len1);
					System.arraycopy(buffer, 0, newArray.buffer, newArray.end
							+ len1, coordinates[index + 1] - len1);
				} else {
					System.arraycopy(buffer, coordinates[index],
							newArray.buffer, newArray.end,
							coordinates[index + 1]);
				}
				newArray.end += coordinates[index + 1];
				newArray.nElements++;
			}
			newArray.copyTo(this);
			newArray.clear();
			fb.release(newArray);

		}

		log.debug("Time repopulate (\t" + indexes.length + "\t):\t"
				+ (System.currentTimeMillis() - time) + "\tT:"
				+ Thread.currentThread().getId());

		int lastIndex = indexes[indexes.length - 1];
		lengthLastElement = coordinates[lastIndex + 1];
		pointerLastElement = end - lengthLastElement;
		if (pointerLastElement < 0) {
			pointerLastElement += buffer.length;
		}
	}

	public void writeElementsTo(DataOutput cacheOutputStream)
			throws IOException {

		if (end > start) {
			cacheOutputStream.write(this.buffer, start, end - start);
		} else if (start > end) {
			cacheOutputStream.write(this.buffer, start, buffer.length - start);
			cacheOutputStream.write(this.buffer, 0, end);
		}
	}

	public boolean addRaw(byte[] key) {

		if (!grow(key.length + 4)) {
			return false;
		}

		try {
			if (enableFieldDelimitors) {
				output.writeInt(key.length);
				pointerLastElement = end;
				output.write(key);
				lengthLastElement = key.length;
			} else {
				output.write(key);
			}
		} catch(IOException e) {
			log.error("Internal error, should not happen", e);
		}

		nElements++;

		return true;
	}

	public boolean addAll(FDataInput originalStream, byte[] lastEl,
			long nElements, long size) throws Exception {

		if (!grow((int) size)) {
			return false;
		}

		if (size > buffer.length - end) {
			originalStream.readFully(buffer, end, buffer.length - end);
			end = (int) (size - buffer.length + end);
			originalStream.readFully(buffer, 0, end);

		} else {
			originalStream.readFully(buffer, end, (int) size);
			end += size;
		}

		this.nElements += nElements;
		if (enableFieldDelimitors && nElements > 0 && lastEl != null) {
			lengthLastElement = lastEl.length;
			pointerLastElement = end - lengthLastElement;
			if (pointerLastElement < 0) {
				pointerLastElement += buffer.length;
			}
		} else {
			pointerLastElement = -1;
		}

		return true;
	}

	public void setFieldsDelimiter(boolean enable) {
		if (nElements > 0 && enableFieldDelimitors != enable) {
			throw new UnsupportedOperationException(
					"setFieldsDelimiter only works on empty buffers");
		}
		enableFieldDelimitors = enable;
	}
}