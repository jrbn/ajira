package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.storage.Writable;

public class Tuple implements Writable {

	private SimpleData[] signature;
	private int nElements = 0;

	protected Tuple() {
	}

	protected Tuple(SimpleData[] data) {
		set(data);
	}

	public int getNElements() {
		return nElements;
	}

	public SimpleData get(int pos) {
		return signature[pos];
	}

	public void set(SimpleData... elements) {
		if (elements != null) {
			// for (int i = 0; i < elements.length; ++i) {
			// set(elements[i], i);
			// }
			signature = elements;
			nElements = elements.length;
		} else {
			nElements = 0;
		}
	}

	public void set(SimpleData el, int pos) {
		if (el == null) {
			return;
		}

		if (signature[pos] == null) {
			signature[pos] = DataProvider.getInstance().get(el.getIdDatatype());
		} else if (signature[pos].getIdDatatype() != el.getIdDatatype()) {
			DataProvider.getInstance().release(signature[pos]);
			signature[pos] = DataProvider.getInstance().get(el.getIdDatatype());
		}

		el.copyTo(signature[pos]);
	}

	public void copyTo(Tuple tuple) {
		Tuple t = tuple;
		t.nElements = nElements;
		if (tuple.signature == null || tuple.signature.length != nElements) {
			tuple.signature = new SimpleData[nElements];
		}
		for (int i = 0; i < nElements; ++i) {
			t.set(signature[i], i);
		}
	}

	public boolean equals(Tuple tuple) {
		if (nElements == tuple.nElements) {
			for (int i = 0; i < nElements; ++i) {
				if (signature[i] == null) {
					if (tuple.signature[i] != null) {
						return false;
					}
				} else if (tuple.signature[i] == null) {
					if (signature[i] != null) {
						return false;
					}
				} else if (signature[i].compareTo(tuple.signature[i]) != 0) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		for (int i = 0; i < nElements; ++i) {
			signature[i].readFrom(input);
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		for (int i = 0; i < nElements; ++i) {
			signature[i].writeTo(output);
		}
	}

	@Override
	public int bytesToStore() throws IOException {
		int size = 0;
		for (int i = 0; i < nElements; ++i) {
			if (signature[i] != null)
				size += signature[i].bytesToStore();
		}
		return size;
	}

	// @Override
	// public int hashCode() {
	// int index = hashCodeFields[0];
	//
	// int s = indexElements[index];
	// int e = indexElements[index + 1];
	//
	// if (e - s >= 4) {
	// return (cb.buffer[e - 1] & 0xff) + ((cb.buffer[e - 2] & 0xff) << 8)
	// + ((cb.buffer[e - 3] & 0xff) << 16)
	// + (cb.buffer[e - 4] << 24);
	// }
	// int hash = 0;
	// for (int i = s; i < e; i++) {
	// hash = (hash << 8) + (cb.buffer[i] & 0xff);
	// }
	// return hash;
	// }
}
