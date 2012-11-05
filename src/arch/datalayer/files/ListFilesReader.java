package arch.datalayer.files;

import java.io.File;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.datalayer.TupleIterator;

public class ListFilesReader extends TupleIterator {

	static final Logger log = LoggerFactory.getLogger(ListFilesReader.class);

	int currentIndex = -1;
	List<File> listFiles = null;
	TString file = new TString();

	public ListFilesReader(String path, String filter) {
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
