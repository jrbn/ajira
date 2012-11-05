package arch.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import arch.utils.Consts;

public final class TInt extends SimpleData {

	int value;

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TINT;
	}
	
	public TInt() {}
	
	public TInt(int i) {
		value = i;
	}

	public int getValue() {
		return value;
	}
	
	public void setValue(int value) {
		this.value = value;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		value = input.readInt();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(value);
	}
	
	@Override
	public int bytesToStore() {
		return 4;
	}
	
	public String toString() {
		return Integer.toString(value);
	}
}