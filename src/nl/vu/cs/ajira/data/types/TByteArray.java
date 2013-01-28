package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import nl.vu.cs.ajira.storage.RawComparator;
import nl.vu.cs.ajira.utils.Consts;

public class TByteArray extends SimpleData {

	byte[] array;

	public byte[] getArray() {
		return array;
	}

	public void setArray(byte[] array) {
		this.array = array;
	}

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TBYTEARRAY;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int s = input.readInt();
		if (s != -1) {
			if (array == null || array.length != s) {
				array = new byte[s];
			}
			input.readFully(array);
		} else {
			array = null;
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		if (array == null) {
			output.writeInt(-1);
		} else {
			output.write(array.length);
			output.write(array);
		}
	}

	@Override
	public int bytesToStore() throws IOException {
		return (array == null) ? 4 : 4 + array.length;
	}

	@Override
	public void copyTo(SimpleData el) {
		if (array != null) {
			((TByteArray) el).array = Arrays.copyOf(array, array.length);
		} else {
			((TByteArray) el).array = null;
		}
	}

	@Override
	public int compareTo(SimpleData el) {
		byte[] array2 = ((TByteArray) el).array;
		if (array == null) {
			if (array2 == null) {
				return 0;
			} else {
				return -1;
			}
		} else {
			if (array2 == null) {
				return 1;
			} else {
				return RawComparator.compareBytes(array, 0, array.length,
						array2, 0, array2.length);
			}
		}
	}
}
