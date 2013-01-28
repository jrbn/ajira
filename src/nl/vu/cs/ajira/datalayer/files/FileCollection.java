package nl.vu.cs.ajira.datalayer.files;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FileCollection implements Serializable {

	private static final long serialVersionUID = -5088467846029256354L;

	private final List<File> files = new ArrayList<File>();
	long totalSize = 0;

	public void addFile(File file) {
		files.add(file);
		totalSize += file.length();
	}

	public List<File> getFiles() {
		return files;
	}

	public long getSize() {
		return totalSize;
	}

	@Override
	public String toString() {
		return "Total size: " + totalSize + " " + files;
	}

	public int getNFiles() {
		return files.size();
	}

	public File getFile(int i) {
		return files.get(i);
	}
}