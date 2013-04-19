package nl.vu.cs.ajira.datalayer.files;

import java.io.File;

import nl.vu.cs.ajira.data.types.Tuple;

public interface FileReader {

	public boolean next();

	public void getTuple(Tuple tuple) throws Exception;

	public void init(File file);

	public void close();

}
