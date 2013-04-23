package nl.vu.cs.ajira.actions;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.files.DefaultFileWriter;
import nl.vu.cs.ajira.datalayer.files.FileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>WriteToFiles</code> action writes its input to a file, by means of
 * a so-called file-writer. A default file-writer is provided, whose
 * functionality is to write to a file, possibly compressed. The file itself is
 * placed in a user-specified output directory.
 */
/**
 * @author jacopo
 * 
 */
public class WriteToFiles extends Action {

	final static Logger log = LoggerFactory.getLogger(WriteToFiles.class);

	/**
	 * The <code>S_CUSTOM_WRITER</code> parameter, of type <code>String</code>,
	 * is not required, and defaults to the class name of
	 * {@link StandardFileWriter}. When supplied, it should indicate a class
	 * name of a class that must extend {@link StandardFileWriter}, and must
	 * have a public constructor with two parameters: an {@link ActionContext},
	 * and a {@link File}.
	 */
	public static final int S_CUSTOM_WRITER = 0;

	/**
	 * The <code>S_OUTPUT_DIR</code> parameter, of type <code>String</code>, is
	 * required, and specifies the directory where the output file(s) are
	 * stored. It should either be a directory, or not exist yet.
	 */
	public static final int S_OUTPUT_DIR = 1;

	/**
	 * This parameter can be used to set a custom prefix of the file names that
	 * are being created.
	 */
	public static final int S_PREFIX_FILE = 2;

	private FileWriter file = null;
	private String outputDirectory = null;
	private String customWriter = null;
	private long count;
	private String prefixFile = null;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_CUSTOM_WRITER, "S_CUSTOM_WRITER", null, false);
		conf.registerParameter(S_OUTPUT_DIR, "S_OUTPUT_DIR", null, true);
		conf.registerParameter(S_PREFIX_FILE, "Prefix of the file", "part",
				false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		outputDirectory = getParamString(S_OUTPUT_DIR);
		customWriter = getParamString(S_CUSTOM_WRITER);
		prefixFile = getParamString(S_PREFIX_FILE);
		file = null;
		count = 0;
	}

	private void openFile(ActionContext context) throws IOException {
		NumberFormat nf = NumberFormat.getInstance();
		nf.setMinimumIntegerDigits(5);
		nf.setGroupingUsed(false);

		// Calculate the filename
		File f = new File(outputDirectory);

		if (!f.exists()) {
			f.mkdirs();
		}

		String filename = prefixFile + "-"
				+ nf.format(context.getCounter("OutputFile")) + "_"
				+ nf.format(0);
		f = new File(f, filename);

		if (customWriter != null) {
			try {
				file = Class.forName(customWriter).asSubclass(FileWriter.class)
						.newInstance();
			} catch (Throwable e) {
				log.error("Could not load class " + customWriter, e);
				throw new IOException("Could not load class " + customWriter, e);
			}
		} else {
			log.debug("No custom writer is specified. Using standard one");
			file = new DefaultFileWriter();
		}

		file.init(f);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		++count;
		if (file == null) {
			openFile(context);
		}
		file.write(inputTuple);
		output.output(inputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		if (file != null) {
			file.close();
		}
		file = null;
		context.incrCounter("Records Written To Files", count);
	}

}
