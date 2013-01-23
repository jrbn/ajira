package nl.vu.cs.ajira.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Writable {

	abstract public void readFrom(DataInput input) throws IOException;

	abstract public void writeTo(DataOutput output) throws IOException;

	abstract public int bytesToStore() throws IOException;

}