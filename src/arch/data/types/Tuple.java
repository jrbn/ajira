package arch.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.data.types.bytearray.BDataInput;
import arch.data.types.bytearray.BDataOutput;
import arch.data.types.bytearray.ByteArray;
import arch.storage.RawComparator;
import arch.storage.Writable;
import arch.utils.Consts;

public class Tuple extends Writable {

    static final Logger log = LoggerFactory.getLogger(Tuple.class);

    private byte[] typeData = new byte[Consts.MAN_N_DATA_IN_TUPLE];
    protected ByteArray cb = new ByteArray();
    protected int nElements = 0;
    protected int[] indexElements = new int[4];
    protected DataOutput output = new BDataOutput(cb);
    protected DataInput input = new BDataInput(cb);

    public static final Tuple EMPTY_TUPLE = new Tuple();

    public Tuple(SimpleData... elements) {
	this();
	try {
	    set(elements);
	} catch (Exception e) {
	    log.error("Error instantiating tuple.", e);
	}
    }

    public Tuple() {
	byte[] b = new byte[Consts.MAX_TUPLE_SIZE];
	cb.setBuffer(b);
    }

    // Constructor with explicit storage.
    // This constructor can only be used when the tuple is initialized
    // completely before calling
    // getEnd(), to figure out where to start the next tuple.
    // Useful when you need to allocate and initialize a lot of tuples, which
    // are further on only
    // used "read-only". In that case, you can allocate a big byte-array once,
    // and let the tuples
    // use that.
    public Tuple(byte[] buf, int start) {
	cb.setBuffer(buf);
	cb.start = start;
	cb.end = start;
    }

    public int getEnd() {
	return cb.end;
    }

    @Override
    public void readFrom(DataInput input) throws IOException {
	nElements = input.readUnsignedByte();

	if (nElements > 0) {
	    cb.end = input.readShort() + cb.start;
	    input.readFully(cb.buffer, cb.start, cb.end - cb.start);
	    if (indexElements == null) {
		indexElements = new int[4];
	    }
	    if (nElements >= indexElements.length) {
		increaseIndexSize(nElements + 1);
	    }
	    // There may be two bytes to be spared here, but then watch out for
	    // arch.storage.TupleComparator!
	    for (int i = 0; i <= nElements; ++i) {
		indexElements[i] = input.readUnsignedByte() + cb.start;
	    }
	    input.readFully(typeData, 0, nElements);
	} else {
	    cb.end = cb.start;
	}
    }

    public int compareTo(Tuple buffer) {
	int len1 = cb.end - cb.start;
	int len2 = buffer.cb.end - buffer.cb.start;
	return RawComparator.compareBytes(cb.buffer, cb.start, len1,
		buffer.cb.buffer, buffer.cb.start, len2);
    }

    public void clear() {
	nElements = 0;
	cb.end = cb.start;
	indexElements[0] = cb.start;
    }

    public int getNElements() {
	return nElements;
    }

    public void removeLast() throws Exception {
	cb.end = indexElements[--nElements];
    }

    private void increaseIndexSize(int nEl) {
	int newSize = 2 * indexElements.length;
	while (nEl >= newSize) {
	    newSize = 2 * newSize;
	}
	int[] newIndex = new int[newSize];
	System.arraycopy(indexElements, 0, newIndex, 0, indexElements.length);
	indexElements = newIndex;
    }

    @Override
    public void writeTo(DataOutput output) throws IOException {
	output.writeByte((byte) nElements);
	if (nElements > 0) {
	    int size = cb.end - cb.start;
	    output.writeShort((short) size);
	    output.write(cb.buffer, cb.start, size);
	    for (int i = 0; i <= nElements; ++i)
		// TODO: cannot get longer than 255 bytes
		output.write(indexElements[i] - cb.start);
	    output.write(typeData, 0, nElements);
	}
    }

    @Override
    public int bytesToStore() {
	return 1
		+ (nElements > 0 ? (2 + (cb.end - cb.start) + nElements + 1)
			: 0) + nElements;
    }

    public static SimpleData getField(byte[] buffer, int start, int position,
	    DataProvider dp) throws IOException {
	int elements = buffer[start];
	if (elements == 0) {
	    return null;
	}

	BDataInput input = new BDataInput(buffer);
	input.setCurrentPosition(start + 1);
	int rawSize = input.readShort();
	int startPosField = start + 3 + rawSize + position;
	int type = buffer[startPosField + elements + 1];
	SimpleData data = dp.get(type);
	input.setCurrentPosition(start + 3 + buffer[startPosField]);
	data.readFrom(input);
	return data;
    }

    public int compare(byte[] buffer, int start) {
	int len = cb.end - cb.start;
	return RawComparator.compareBytes(this.cb.buffer, this.cb.start, len,
		buffer, start + 3, len);
    }

    public boolean get(SimpleData element, int index) throws Exception {
	if (index >= nElements) {
	    return false;
	}

	int originalStart = cb.start;
	cb.start = indexElements[index];
	element.readFrom(input);
	cb.start = originalStart;
	return true;
    }

    public int getType(int i) {
	return typeData[i];
    }

    public void get(SimpleData... elements) throws Exception {
        int originalStart = cb.start;
	for (int i = 0; i < elements.length; ++i) {
	    if (i >= nElements) {
	        break;
	    }
	    cb.start = indexElements[i];
	    elements[i].readFrom(input);
	}
	cb.start = originalStart;
    }

    public void set(SimpleData... elements) throws Exception {
 	nElements = 0;
	cb.end = cb.start;
	indexElements[0] = cb.start;
	int capacity = remainingCapacity();
	if (elements.length >= indexElements.length) {
	    increaseIndexSize(elements.length);
	}
	for (SimpleData element : elements) {
	    int len = element.bytesToStore();

	    if (len > capacity) {
	        log.error("Tuple not large enough");
	        return;
	    }
	    capacity -= len;
	    element.writeTo(output);
	    typeData[nElements] = (byte) element.getIdDatatype();
	    nElements++;
	    indexElements[nElements] = cb.end;
	}
    }

    public String toString(DataProvider dp) {
	try {
	    String value = "";
	    for (int i = 0; i < nElements; ++i) {
		SimpleData data = dp.get(typeData[i]);
		get(data, i);
		value += "<" + data.toString() + "> ";
		dp.release(data);
	    }
	    value = value.trim();
	    return value;
	} catch (Exception e) {
	    log.error("Error parsing tuple", e);
	}

	return null;
    }

    public void copyTo(Tuple buffer) {
	buffer.nElements = nElements;

	buffer.cb.end = buffer.cb.start + cb.end - cb.start;

	System.arraycopy(cb.buffer, cb.start, buffer.cb.buffer,
		buffer.cb.start, cb.end - cb.start);

	if (buffer.indexElements == null
		|| buffer.indexElements.length < indexElements.length) {
	    buffer.indexElements = new int[indexElements.length];
	}
	for (int i = 0; i < indexElements.length; i++) {
	    buffer.indexElements[i] = buffer.cb.start + indexElements[i]
		    - cb.start;
	}
	System.arraycopy(typeData, 0, buffer.typeData, 0, nElements);
    }

    public void concat(DataProvider dp, Tuple tuple) throws Exception {
	for (int i = 0; i < tuple.nElements; ++i) {
	    SimpleData d = dp.get(tuple.getType(i));
	    tuple.get(d, i);
	    add(d);
	    dp.release(d);
	}
    }

    public boolean add(SimpleData element) throws Exception {
	int len = element.bytesToStore();

	if (len > remainingCapacity()) {
	    return false;
	}
	element.writeTo(output);

	nElements++;
	if (nElements >= indexElements.length) {
	    increaseIndexSize(nElements + 1);
	}
	indexElements[nElements] = cb.end;

	typeData[nElements - 1] = (byte) element.getIdDatatype();
	return true;
    }

    public int remainingCapacity() {
	return cb.buffer.length - cb.end;
    }

    public void addRaw(Tuple buffer, int i) throws Exception {
	int start = buffer.indexElements[i];
	int length = buffer.indexElements[i + 1] - buffer.indexElements[i];

	System.arraycopy(buffer.cb.buffer, start, cb.buffer, cb.end, length);
	cb.end += length;
	nElements++;
	if (nElements >= indexElements.length) {
	    increaseIndexSize(nElements + 1);
	}
	indexElements[nElements] = cb.end;
	typeData[nElements - 1] = buffer.typeData[i];
    }

    // ***** The next three methods are used for the HashJoin *****

    private int[] hashCodeFields = null;

    public void setHashCodeFields(int[] fields) {
	hashCodeFields = fields;
    }

    @Override
    public boolean equals(Object obj) {
	try {
	    Tuple other = (Tuple) obj;
	    for (int i = 0; i < hashCodeFields.length; i++) {
		// Replaced call to compareElement with inline code that
		// only compares for equality. --Ceriel
		int el1 = hashCodeFields[i];
		int s1 = indexElements[el1];
		int end1 = indexElements[el1 + 1];
		int el2 = other.hashCodeFields[i];
		int s2 = other.indexElements[el2];
		int end2 = other.indexElements[el2 + 1];
		while (s1 < end1 && s2 < end2) {
		    if (cb.buffer[s1] != other.cb.buffer[s2]) {
			return false;
		    }
		    ++s1;
		    ++s2;
		}
		if (end1 - s1 != end2 - s2) {
		    return false;
		}
	    }
	    return true;
	} catch (Exception e) {
	    log.error("Error in the comparison", e);
	}

	return false;

    }

    public int compareElement(int i, Tuple buffer, int j) throws Exception {

	int start = indexElements[i];
	int length = indexElements[i + 1] - start;

	int start2 = buffer.indexElements[j];
	int length2 = buffer.indexElements[j + 1] - start2;

	return RawComparator.compareBytes(cb.buffer, start, length,
		buffer.cb.buffer, start2, length2);
    }

    @Override
    public int hashCode() {
	int index = hashCodeFields[0];

	int s = indexElements[index];
	int e = indexElements[index + 1];

	if (e - s >= 4) {
	    return (cb.buffer[e - 1] & 0xff) + ((cb.buffer[e - 2] & 0xff) << 8)
		    + ((cb.buffer[e - 3] & 0xff) << 16)
		    + (cb.buffer[e - 4] << 24);
	}
	int hash = 0;
	for (int i = s; i < e; i++) {
	    hash = (hash << 8) + (cb.buffer[i] & 0xff);
	}
	return hash;
    }

    public int getHash(int maxBytes) {
	int hash = 0;
	for (int i = cb.start; maxBytes > 0 && i < cb.end; i++, maxBytes--) {
	    hash = 31 * hash + (cb.buffer[i] & 0xff);
	}
	return hash;
    }

    public int getHash(int index, int maxBytes) throws Exception {
	int s = indexElements[index];
	int hash = 0;
	for (int i = s; maxBytes > 0 && i < cb.end; i++, maxBytes--) {
	    hash = 31 * hash + (cb.buffer[i] & 0xff);
	}
	return hash;
    }
}
