package arch.actions.files;

import java.io.File;
import java.io.FilenameFilter;

public class FilterHiddenFiles implements FilenameFilter {

	@Override
	public boolean accept(File dir, String name) {
		return !name.startsWith(".") && !name.startsWith("_");
	}
}
