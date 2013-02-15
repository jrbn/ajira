package nl.vu.cs.ajira.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface Writable {

	public void readFrom(DataInput input) throws IOException;

	public void writeTo(DataOutput output) throws IOException;
}

