package arch.datalayer.files;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.data.types.Tuple;
import arch.datalayer.TupleIterator;

public class FilesIterator extends TupleIterator {

	static final Logger log = LoggerFactory.getLogger(FilesIterator.class);

	int currentIndex = 0;
	FileCollection files = null;
	Constructor<? extends FileParser> cfileReader;
	FileParser currentItr = null;

	public FilesIterator(FileCollection files,
			Class<? extends FileParser> cfileReader)
			throws SecurityException, NoSuchMethodException, IOException {
		this.cfileReader = cfileReader.getConstructor(File.class);
		log.debug("Input: " + files);
		this.files = files;
	}

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

	@Override
	public void getTuple(Tuple tuple) throws Exception {
		currentItr.getTuple(tuple);
	}

	@Override
	public boolean isReady() {
		return true;
	}
}
