package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.utils.Consts;

public class TDouble extends SimpleData {

	private double value;

	@Override
	public void readFrom(DataInput input) throws IOException {
		value = input.readDouble();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeDouble(value);
	}

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TDOUBLE;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public double getValue() {
		return value;
	}

	@Override
	public void copyTo(SimpleData el) {
		((TDouble) el).value = value;
	}

	@Override
	public int compareTo(SimpleData el) {
		double diff = this.value - ((TDouble) el).value;
		if (diff < 0) {
			return -1;
		} else if (diff > 0) {
			return 1;
		}
		return 0;
	}

	@Override
	public boolean equals(SimpleData el, ActionContext context) {
		return ((TDouble) el).value == this.value;
	}

}
