package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.utils.Consts;

public class TBag extends SimpleData {

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TBYTE;
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
}
