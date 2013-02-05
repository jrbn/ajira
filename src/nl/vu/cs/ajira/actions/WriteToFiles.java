package nl.vu.cs.ajira.actions;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.NumberFormat;

import nl.vu.cs.ajira.data.types.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteToFiles extends Action {

	final static Logger log = LoggerFactory.getLogger(WriteToFiles.class);

	public static final int CUSTOM_WRITER = 0;
	public static final String S_CUSTOM_WRITER = "custom_writer";
	public static final int OUTPUT_DIR = 1;
	public static final String S_OUTPUT_DIR = "output_dir";

	static public class StandardFileWriter {

		FileWriter writer = null;

		public StandardFileWriter(ActionContext context, File file)
				throws IOException {
			writer = new FileWriter(file);
		}

		public StandardFileWriter() {
		}

		public void write(Tuple tuple) throws Exception {
			if (tuple.getNElements() > 0) {
				String value = tuple.get(0).toString();
				for (int i = 1; i < tuple.getNElements(); ++i) {
					value += " " + tuple.get(i).toString();
				}
				writer.write(value + "\n");
			}
		}

		public void close() throws IOException {
			writer.close();
		}
	}

	StandardFileWriter file = null;
	String outputDirectory = null;
	String customWriter = null;
	long count;

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(CUSTOM_WRITER, S_CUSTOM_WRITER, null, false);
		conf.registerParameter(OUTPUT_DIR, S_OUTPUT_DIR, null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		outputDirectory = getParamString(OUTPUT_DIR);
		customWriter = getParamString(CUSTOM_WRITER);
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
			f.mkdir();
		}

		f = new File(f, "part-" + nf.format(context.getCounter("OutputFile"))
				+ "_" + nf.format(0));

		try {
			if (customWriter != null) {
				Constructor<? extends StandardFileWriter> constr = Class
						.forName(customWriter)
						.asSubclass(StandardFileWriter.class)
						.getConstructor(ActionContext.class, File.class);
				file = constr.newInstance(context, f);
			} else {
				log.debug("No custom writer is specified. Using standard one");
				file = new StandardFileWriter(context, f);
			}
		} catch (Exception e) {
			log.error("Error instantiating writer for file " + file + "("
					+ customWriter + ")", e);
			file = null;
		}
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		++count;
		if (file == null) {
			openFile(context);
		}
		file.write(inputTuple);
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
