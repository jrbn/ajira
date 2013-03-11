package nl.vu.cs.ajira.datalayer.files;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class provides an iterator over a collection of 
 * files. It reads one line from the current file at each
 * call of the method next(); 
 *
 */
public class FilesIterator extends TupleIterator {

	static final Logger log = LoggerFactory.getLogger(FilesIterator.class);

	int currentIndex = 0;
	FileCollection files = null;
	Constructor<? extends DefaultFileParser> cfileReader;
	DefaultFileParser currentItr = null;

	/**
	 * Custom constructor.
	 * 
	 * @param files
	 * 		The new FileCollection. 
	 * @param cfileReader
	 * 		The class that will provide the constructor 
	 * 		for the files in the collection.
	 * 
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws IOException
	 */
	public FilesIterator(FileCollection files,
			Class<? extends DefaultFileParser> cfileReader)
			throws SecurityException, NoSuchMethodException, IOException {
		this.cfileReader = cfileReader.getConstructor(File.class);
		log.debug("Input: " + files);
		this.files = files;
	}

	/**
	 * Returns true if exists a new line in the current file 
	 * or it exists a next file. It returns false otherwise.
	 * In case it is updated the current file with the next
	 * file for the collection the method it is called again.
	 */
	@Override
	public boolean next() throws Exception {
		if (currentItr == null || !currentItr.next()) {
			if (currentIndex < files.getNFiles()) {
				File file = files.getFile(currentIndex++);
				currentItr = cfileReader.newInstance(file);
				if (currentItr != null) {
					return next();
				} else {
					return false;
				}
			}
			return false;
		}

		return true;
	}

	/**
	 * Updates the filed of the tuple with the last line 
	 * read form the current file.
	 */
	@Override
	public void getTuple(Tuple tuple) throws Exception {
		currentItr.getTuple(tuple);
	}

	@Override
	public boolean isReady() {
		return true;
	}
}
