package arch.datalayer.files;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {

	static final Logger log = LoggerFactory.getLogger(FileUtils.class);

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

	public static boolean isSymLink(File file) throws IOException {
		return !file.getAbsolutePath().equals(file.getCanonicalPath());
	}

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
