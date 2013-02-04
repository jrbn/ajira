package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import nl.vu.cs.ajira.utils.Consts;

public class TBag extends SimpleData implements Iterable<Tuple> {

	Iterator<Tuple> internalItr;

	public TBag(Iterator<Tuple> itr) {
		internalItr = itr;
	}

	public TBag() {
	}

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TBAG;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		throw new IOException("Not (yet) implemented");
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		throw new IOException("Not (yet) implemented");
	}

	@Override
	public void copyTo(SimpleData el) {
		((TBag) el).internalItr = internalItr;
	}

	@Override
	public int compareTo(SimpleData el) {
		if (((TBag) el).internalItr == internalItr) {
			return 0;
		} else {
			return -1;
		}
	}

	@Override
	public Iterator<Tuple> iterator() {
		return internalItr;
	}

	@Override
	public boolean equals(SimpleData el) {
		return super.equals((Object) el);
	}
}
