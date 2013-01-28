package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.utils.Consts;

public class TBag extends SimpleData {

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TBAG;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		// TODO
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		// TODO
	}

	@Override
	public int bytesToStore() {
		// TODO
		return -1;
	}

	@Override
	public void copyTo(SimpleData el) {
		// TODO Auto-generated method stub

	}

	@Override
	public int compareTo(SimpleData el) {
		// TODO Auto-generated method stub
		return 0;
	}
}
