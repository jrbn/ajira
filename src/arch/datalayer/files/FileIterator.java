package arch.datalayer.files;

import java.io.File;

import arch.datalayer.TupleIterator;

abstract public class FileIterator extends TupleIterator {
	
	@Override
	public boolean isReady() {
		return true;
	}
	
	public void waitUntilReady() {
		return;
	}

	public FileIterator(File file) {
	}

}
