package nl.vu.cs.ajira.datalayer.files;

import java.io.File;
import java.io.IOException;
import java.util.List;

import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class provides an iterator for a 
 * list on files.
 *
 */
public class ListFilesIterator extends TupleIterator {

	static final Logger log = LoggerFactory.getLogger(ListFilesIterator.class);

	int currentIndex = -1;
	List<File> listFiles = null;
	TString file = new TString();

	/**
	 * Custom constructor.
	 * 
	 * @param path
	 * 		The pathname of the file/directory.
	 * @param filter
	 * 		The name of the filterClass that provides 
	 * 		the rules that the files have to satisfy.
	 * @throws IOException 
	 */
	public ListFilesIterator(String path, String filter) throws IOException {
		listFiles = FileUtils.listAllFiles(path, filter);
	}

	/**
	 * It increases the currentIndex and returns true
	 * if exists a next file and false otherwise.
	 */
	@Override
	public boolean next() throws Exception {
		currentIndex++;
		return listFiles != null && currentIndex < listFiles.size();
	}

	/**
	 * Sets the value of the tuple to be the 
	 * absolute path of the current file.
	 */
	@Override
	public void getTuple(Tuple tuple) throws Exception {
		file.setValue(listFiles.get(currentIndex).getAbsolutePath());
		tuple.set(file);
	}

	@Override
	public boolean isReady() {
		return true;
	}
}
