package nl.vu.cs.ajira.actions.support;

import java.io.File;
import java.io.FilenameFilter;

/**
 * A simple filter for hidden filenames.
 */
public class FilterHiddenFiles implements FilenameFilter {

	/**
	 * Only accept filenames that don't start with a '.' or '_'.
	 * @param dir
	 * 		the directory in which the file was found
	 * @param name
	 * 		the name of the file
	 * @return
	 * 		<code>true</code> if the name of the file does not start with a '.' or '_',
	 * 		<code>false</code> otherwise.
	 */
	@Override
	public boolean accept(File dir, String name) {
		return !name.startsWith(".") && !name.startsWith("_");
	}
}
