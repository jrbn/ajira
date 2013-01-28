package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.storage.Writable;
import nl.vu.cs.ajira.utils.Consts;

public class Tuple implements Writable {
	private final SimpleData[] array = new SimpleData[Consts.MAN_N_DATA_IN_TUPLE];
	private int nElements = 0;

	Tuple() {
	}

	Tuple(SimpleData[] data) {
		set(data);
	}

	public int getNElements() {
		return nElements;
	}

	public SimpleData get(int pos) {
		return array[pos];
	}

	public void set(SimpleData... elements) {
		if (elements != null) {
			for (int i = 0; i < elements.length; ++i) {
				set(elements[i], i);
			}
			nElements = elements.length;
		} else {
			nElements = 0;
		}
	}

	public void set(SimpleData el, int pos) {
		if (el == null) {
			return;
		}

		if (array[pos] == null) {
			array[pos] = DataProvider.getInstance().get(el.getIdDatatype());
		} else if (array[pos].getIdDatatype() != el.getIdDatatype()) {
			DataProvider.getInstance().release(array[pos]);
			array[pos] = DataProvider.getInstance().get(el.getIdDatatype());
		}

		el.copyTo(array[pos]);
	}

	public void add(SimpleData el) {
		set(el, nElements++);
	}

	public void copyTo(Tuple tuple) {
		Tuple t = tuple;
		t.nElements = nElements;
		for (int i = 0; i < nElements; ++i) {
			t.set(array[i], i);
		}
	}

	public boolean equals(Tuple tuple) {
		if (nElements == tuple.nElements) {
			for (int i = 0; i < nElements; ++i) {
				if (array[i] == null) {
					if (tuple.array[i] != null) {
						return false;
					}
				} else if (tuple.array[i] == null) {
					if (array[i] != null) {
						return false;
					}
				} else if (array[i].compareTo(tuple.array[i]) != 0) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		nElements = input.readInt();
		DataProvider dp = DataProvider.getInstance();
		for (int i = 0; i < nElements; ++i) {
			int t = input.readByte();
			if (t != -1) {
				if (array[i] == null || array[i].getIdDatatype() != t)
					array[i] = dp.get(t);
				array[i].readFrom(input);
			} else {
				array[i] = null;
			}
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(nElements);
		for (int i = 0; i < nElements; ++i) {
			if (array[i] != null) {
				output.writeByte(array[i].getIdDatatype());
				array[i].writeTo(output);
			} else {
				output.writeByte(-1);
			}
		}
	}

	@Override
	public int bytesToStore() throws IOException {
		int size = 4;
		for (int i = 0; i < nElements; ++i) {
			if (array[i] != null)
				size += array[i].bytesToStore();
		}
		return size;
	}
}
