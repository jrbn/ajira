package nl.vu.cs.ajira.datalayer.files;

import java.io.File;
import java.util.List;

import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ListFilesIterator extends TupleIterator {

	static final Logger log = LoggerFactory.getLogger(ListFilesIterator.class);

	int currentIndex = -1;
	List<File> listFiles = null;
	TString file = new TString();

	public ListFilesIterator(String path, String filter) {
		try {
			listFiles = FileUtils.listAllFiles(path, filter);
		} catch (Exception e) {
			log.error("Error", e);
		}
	}

	@Override
	public boolean next() throws Exception {
		currentIndex++;
		return listFiles != null && currentIndex < listFiles.size();
	}

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
