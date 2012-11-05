package arch.test;

import arch.Arch;
import arch.datalayer.files.FilesLayer;
import arch.datalayer.files.LineTextFilesReader;
import arch.utils.Configuration;
import arch.utils.Consts;

public class StartupCluster {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Configuration conf = new Configuration();
		conf.set(Consts.STORAGE_IMPL, FilesLayer.class.getName());
		conf.set(FilesLayer.IMPL_FILE_READER,
				LineTextFilesReader.class.getName());
		conf.setBoolean(Consts.START_IBIS, true);

		Arch arch = new Arch();
		arch.startup(conf);
	}
}
