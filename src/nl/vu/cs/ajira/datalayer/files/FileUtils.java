package nl.vu.cs.ajira.datalayer.files;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class it is used to provide a list of files 
 * found at a given path and that satisfy a filter.  
 *
 */
public class FileUtils {

	static final Logger log = LoggerFactory.getLogger(FileUtils.class);

	/**
	 * If the parameter file is a file then it is added at the 
	 * list of files. If it is a directory, it adds recursively
	 * the files from it. In case a filter is provided, the 
	 * files from the directory have to satisfy that filter. 
	 * 
	 * @param list
	 * 		The list of the files.
	 * @param file
	 * 		The file that is added at the list of files or a 
	 * 		directory whose files will be added at the list.
	 * @param filter
	 * 		The filter that it is used to filter the files 
	 * 		from the directory.
	 * 		
	 */
	private static void recursiveListint(List<File> list, File file,
			FilenameFilter filter) {
		if (file.isFile()) {
			list.add(file);
		} else {
			File[] children = null;
			if (filter != null) {
				children = file.listFiles(filter);
			} else {
				children = file.listFiles();
			}

			for (File child : children) {
				if (child.isFile()) {
					list.add(child);
				} else {
					recursiveListint(list, child, filter);
				}
			}
		}
	}

	/**
	 * 
	 * @param file
	 * 		The file that is checked if it has 
	 * 		a canonical path.
	 * @return
	 * 		True if the absolute path of the file 
	 * 		differs from the canonical path, false
	 * 		otherwise.
	 * @throws IOException
	 */
	public static boolean isSymLink(File file) throws IOException {
		return !file.getAbsolutePath().equals(file.getCanonicalPath());
	}

	/**
	 * 
	 * @param path
	 * 		The pathname string.
	 * @param filterClass
	 * 		The name of the filterClass that is used.
	 * @return
	 * 		The list of the files that are found at the 
	 * 		location path and that satisfy the filter 
	 * 		provided. 
	 * @throws IOException
	 */
	public static List<File> listAllFiles(String path, final String filterClass)
			throws IOException {
		List<File> list = new ArrayList<File>();
		File file = new File(path);

		if (isSymLink(file)) {
			// Dereference it to the absolute file.
			file = file.getCanonicalFile();
		}

		if (file.isDirectory()) {
			// Get all files
			if (filterClass != null) {
				try {
					Class<? extends FilenameFilter> clazz = Class.forName(
							filterClass).asSubclass(FilenameFilter.class);
					recursiveListint(list, file, clazz.newInstance());
				} catch (Exception e) {
					log.error("Couldn't instantiate filter " + filterClass
							+ ". Ignore it.");
					recursiveListint(list, file, null);
				}
			} else {
				recursiveListint(list, file, null);
			}

		} else if (file.exists()) {
			list.add(file);
		}
		return list;
	}
}
