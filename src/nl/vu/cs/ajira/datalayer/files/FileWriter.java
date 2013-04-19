package nl.vu.cs.ajira.datalayer.files;

import java.io.File;
import java.io.IOException;

import nl.vu.cs.ajira.data.types.Tuple;

public interface FileWriter {

	public void init(File file);

	public void write(Tuple inputTuple) throws IOException;

	public void close() throws IOException;
}
