package nl.vu.cs.ajira.storage.containers;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Comparator;

import nl.vu.cs.ajira.buckets.TupleComparator;
import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.data.types.bytearray.BDataOutput;
import nl.vu.cs.ajira.data.types.bytearray.ByteArray;
import nl.vu.cs.ajira.data.types.bytearray.CBDataInput;
import nl.vu.cs.ajira.data.types.bytearray.CBDataOutput;
import nl.vu.cs.ajira.data.types.bytearray.FDataInput;
import nl.vu.cs.ajira.data.types.bytearray.FDataOutput;
import nl.vu.cs.ajira.storage.Container;
import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.RawComparator;
import nl.vu.cs.ajira.storage.Writable;
import nl.vu.cs.ajira.utils.Utils;

import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import java.util.zip.Deflater;
//import java.util.zip.DeflaterOutputStream;
//import java.util.zip.Inflater;
//import java.util.zip.InflaterInputStream;

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
	 *            Influences the class used for the input and output fields.
	 * @param enableFieldMarks
	 *            Is the new value of enableFieldDelimitors.
	 * @param maxSize
	 *            The maximum size of the ByteArray's buffer.
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
	 * Constructs a new object and uses CBDataInput class for the input and
	 * output fields.
	 * 
	 * @param enableFieldMarks
	 *            Is the new value of enableFieldDelimitors.
	 * @param size
	 *            The maximum size of the ByteArray's buffer.
	 */
	public WritableContainer(Boolean enableFieldMarks, Integer size) {
		this(new Boolean(true), enableFieldMarks, size);
	}

	/**
	 * Constructs a new object. Uses CBDataInput class for the input and output
	 * fields and set the enableFieldDelimitors to false.
	 * 
	 * @param size
	 *            The maximum size of the ByteArray's buffer.
	 */
	public WritableContainer(Integer size) {
		this(true, false, size);
	}

	/**
	 * Reads from the input the number of elements, if it should enable
	 * delimiters and the elements of the buffer.
	 */
	@Override
	public void readFrom(DataInput input) throws IOException {
		nElements = (int) input.readLong();
		enableFieldDelimitors = input.readBoolean();
		start = 0;
		end = 0;
		if (nElements > 0) {
			super.readFrom(input);
		}
		pointerLastElement = -1;
	}

	/**
	 * Writes in the output the number of elements, if the delimiters are
	 * enabled and the elements of the buffer.
	 */
	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeLong(nElements);
		output.writeBoolean(enableFieldDelimitors);
		if (nElements > 0) {
			super.writeTo(output);
		}
	}

	public int compressTo(byte[] value) {
		Utils.encodeInt(value, 0, nElements);
		value[4] = (byte) (enableFieldDelimitors ? 1 : 0);
		int size = 0;
		if (nElements > 0) {
			size = super.compress(value, 5);
		}
		return 5 + size;
	}

	public void decompressFrom(byte[] value, int length) {
		nElements = Utils.decodeInt(value, 0);
		enableFieldDelimitors = value[4] == 1;
		start = 0;
		end = 0;
		if (nElements > 0) {
			super.decompress(value, 5, length - 5);
		}
		pointerLastElement = -1;
		// checkConsistency();
	}

	public void init(boolean fieldMarks) {
		clear();
		this.enableFieldDelimitors = fieldMarks;
	}

	@Override
	public void clear() {
		nElements = 0;
		pointerLastElement = -1;
		super.clear();
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
					length = end + getTotalCapacity() - tmpPointerLastElement;
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
		if (enableFieldDelimitors != buffer.enableFieldDelimitors
				&& nElements > 0) {
			throw new UnsupportedOperationException(
					"addAll works only if the two buffers share the parameter FieldDelimiters");
		}

		// buffer.checkConsistency();
		// checkConsistency();

		int totalCapacity = getTotalCapacity();
		int necessarySpace = totalCapacity - 1
				- buffer.remainingCapacity(totalCapacity);
		// -1: see code in remainingCapacity --Ceriel
		if (!grow(necessarySpace)) {
			if (log.isDebugEnabled()) {
				log.debug("Grow() fails: necessarySpace = " + necessarySpace);
			}
			return false;
		}

		readFrom(buffer);

		nElements += buffer.getNElements();
		enableFieldDelimitors = buffer.enableFieldDelimitors;

		if (buffer.pointerLastElement >= 0) {
			lengthLastElement = buffer.lengthLastElement;
			pointerLastElement = end - lengthLastElement;
			if (pointerLastElement < 0) {
				pointerLastElement += getTotalCapacity();
			}
		} else {
			pointerLastElement = -1;
		}
		// checkConsistency();
		return true;
	}

	@Override
	public boolean remove(K element) {

		if (start == end) {
			// Container is empty.
			return false;
		}

		if (enableFieldDelimitors) {
			// Skip the length of the just read element
			input.skipBytes(4);
		}

		if (log.isDebugEnabled()) {
			if (nElements <= 0) {
				log.error("OOPS: remove on empty container? start = " + start
						+ ", end = " + end);
				throw new Error("Inconsistency found");
			}
		}

		try {
			element.readFrom(input);
		} catch (IOException e) {
			log.error("Should not happen", e);
			throw new Error("Got unexpected exception", e);
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

	private void checkConsistency() {
		if (end < 0) {
			log.error("checkConsistency: end = " + end, new Throwable());
			throw new Error("Inconsistency found");
		}
		if (enableFieldDelimitors) {
			if (start != end) {
				int savedStart = start;
				int nEls = nElements;
				byte[] v = new byte[1];
				while (nEls > 0) {
					int length = input.readInt();
					if (length < 0 || length > 4096) {
						log.error("Inconsistency, length = " + length,
								new Throwable());
						throw new Error("Inconsistency found");
					}
					if (v.length != length) {
						v = new byte[length];
					}
					input.readFully(v);
					--nEls;
				}
				if (start != end) {
					log.error("Inconsistency, start(" + start + ") != end("
							+ end + ")", new Throwable());
					throw new Error("Inconsistency found");
				}
				start = savedStart;
			}
		} else {
			// TODO
		}
	}

	public byte[] getLastElement() {
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

	public void copyFrom(WritableContainer<?> buffer) {
		enableFieldDelimitors = buffer.enableFieldDelimitors;
		nElements = buffer.nElements;
		start = buffer.start;
		end = buffer.end;
		lengthLastElement = buffer.lengthLastElement;
		pointerLastElement = buffer.pointerLastElement;
		this.buffer = buffer.buffer;
	}

	public void sort(final RawComparator<K> c, Factory<WritableContainer<K>> fb)
			throws Exception {

		if (!enableFieldDelimitors) {
			throw new UnsupportedOperationException(
					"Sorting doesn't work if the flag enableFieldDelimiters is set to false");
		}

		// 1) Populate
		long time = System.currentTimeMillis();
		int size = getRawSize();
		int capacity = getTotalCapacity();

		int l = 0;
		final int[] coordinates = new int[(nElements * 2)];
		Integer[] indexes = new Integer[nElements];

		int i = 0;
		while (nElements > 0) {
			int length = input.readInt();
			coordinates[l] = start;
			coordinates[l + 1] = length;
			start = (coordinates[l] + coordinates[l + 1]) % capacity;
			nElements--;
			indexes[i] = i * 2;
			i++;
			l += 2;
		}
		if (log.isDebugEnabled()) {
			log.debug("Time populate (\t" + indexes.length + "\t):\t"
					+ (System.currentTimeMillis() - time) + "\tT:"
					+ Thread.currentThread().getId());
		}

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

		if (log.isDebugEnabled()) {
			log.debug("Time sorting (\t" + indexes.length + "\t):\t"
					+ (System.currentTimeMillis() - time) + "\tT:"
					+ Thread.currentThread().getId() + "-"
					+ ((TupleComparator) c).timeConverting);
		}

		// 3) Repopulate
		time = System.currentTimeMillis();
		if (size < capacity / 2) {
			for (int index : indexes) {
				try {
					output.writeInt(coordinates[index + 1]);
				} catch (IOException e) {
					log.error("Internal error, should not happen", e);
					throw new Error("Got unexpected exception", e);
				}
				copyRegion(this, coordinates[index], coordinates[index + 1]);
				nElements++;
			}
		} else { // It's too big. Must use another array
			WritableContainer<K> newArray = fb.get();
			newArray.init(enableFieldDelimitors);
			if (newArray.grow(size)) {
				for (int index : indexes) {
					try {
						newArray.output.writeInt(coordinates[index + 1]);
					} catch (IOException e) {
						log.error("Internal error, should not happen", e);
						throw new Error("Got unexpected exception", e);
					}
					newArray.copyRegion(this, coordinates[index],
							coordinates[index + 1]);
					newArray.nElements++;
				}
				copyFrom(newArray); // Uses the buffer from newArray, so don't
									// recycle it.
				newArray = null;
			} else {
				// I don't need it
				newArray.clear();
				fb.release(newArray);
				newArray = null;

				// Not enough space to create an additional array. Store the
				// data on disk and reread it.
				File cacheFile = File.createTempFile("cache", "tmp");
				cacheFile.deleteOnExit();

				OutputStream fout = new SnappyOutputStream(
						new BufferedOutputStream(
								new FileOutputStream(cacheFile)));
				// OutputStream fout = new DeflaterOutputStream(
				// new FileOutputStream(cacheFile), new Deflater(2), 65536);
				FDataOutput cacheOutputStream = new FDataOutput(fout);

				// Copy all stuff on the file
				int s = 0;
				for (int index : indexes) {
					s += 4 + coordinates[index + 1];
					cacheOutputStream.writeInt(coordinates[index + 1]);
					writeTo(cacheOutputStream, coordinates[index],
							coordinates[index + 1]);
				}
				cacheOutputStream.close();

				// Reread file
				InputStream fin = new SnappyInputStream(
						new BufferedInputStream(new FileInputStream(cacheFile)));
				// InputStream fin = new InflaterInputStream(new
				// FileInputStream(
				// cacheFile), new Inflater(), 65536);
				FDataInput fInputCache = new FDataInput(fin);
				start = 0;
				end = s;
				readFrom(fInputCache);
				cacheFile.delete();
				nElements = indexes.length;
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("Time repopulate (\t" + indexes.length + "\t):\t"
					+ (System.currentTimeMillis() - time) + "\tT:"
					+ Thread.currentThread().getId());
		}

		int lastIndex = indexes[indexes.length - 1];
		lengthLastElement = coordinates[lastIndex + 1];
		pointerLastElement = end - lengthLastElement;
		if (pointerLastElement < 0) {
			pointerLastElement += getTotalCapacity();
		}
	}

	public void writeElementsTo(DataOutput cacheOutputStream)
			throws IOException {
		writeRaw(cacheOutputStream);
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
		} catch (IOException e) {
			log.error("Internal error, should not happen", e);
			throw new Error("Got unexpected exception", e);
		}

		nElements++;

		return true;
	}

	public boolean addAll(FDataInput originalStream, byte[] lastEl,
			long nElements, long size) throws Exception {
		// checkConsistency();
		int sz = (int) size;
		if (sz != size || !grow(sz)) {
			return false;
		}

		readFrom(originalStream, sz);

		this.nElements += nElements;
		if (enableFieldDelimitors && nElements > 0 && lastEl != null) {
			lengthLastElement = lastEl.length;
			pointerLastElement = end - lengthLastElement;
			if (pointerLastElement < 0) {
				pointerLastElement += getTotalCapacity();
			}
		} else {
			pointerLastElement = -1;
		}
		// checkConsistency();
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
