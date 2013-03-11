package nl.vu.cs.ajira.datalayer.files;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a collection of files and provides 
 * the methods that are needed to manipulate the collection. 
 *
 */
public class FileCollection implements Serializable {

	private static final long serialVersionUID = -5088467846029256354L;

	private final List<File> files = new ArrayList<File>();
	long totalSize = 0;

	/**
	 * Adds a file to the list.
	 * 
	 * @param file
	 * 		The file that is added to the list 
	 * 		of files.
	 */
	public void addFile(File file) {
		files.add(file);
		totalSize += file.length();
	}

	/**
	 * 
	 * @return
	 * 		The list of files.
	 */
	public List<File> getFiles() {
		return files;
	}

	/**
	 * 
	 * @return
	 * 		The total size of the files 
	 * 		from the collection.
	 */
	public long getSize() {
		return totalSize;
	}

	/**
	 * Returns the String representation of the Object.
	 */
	@Override
	public String toString() {
		return "Total size: " + totalSize + " " + files;
	}

	/**
	 * 
	 * @return
	 * 		The number of files from the collection.
	 */
	public int getNFiles() {
		return files.size();
	}

	/**
	 * 
	 * @param i
	 * 		The position of the file that it is looked.
	 * @return
	 * 		The file that is in collection at the position i.
	 */
	public File getFile(int i) {
		return files.get(i);
	}
}