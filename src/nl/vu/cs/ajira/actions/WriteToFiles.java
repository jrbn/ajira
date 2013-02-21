package nl.vu.cs.ajira.actions;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.NumberFormat;
import java.util.zip.GZIPOutputStream;

import nl.vu.cs.ajira.data.types.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteToFiles extends Action {

	final static Logger log = LoggerFactory.getLogger(WriteToFiles.class);

	public static final int CUSTOM_WRITER = 0;
	private static final String S_CUSTOM_WRITER = "custom_writer";
	public static final int OUTPUT_DIR = 1;
	private static final String S_OUTPUT_DIR = "output_dir";

	static public class StandardFileWriter {

		BufferedOutputStream writer = null;

		public StandardFileWriter(ActionContext context, File file,
				boolean compressGZip) throws IOException {
			if (compressGZip) {
				writer = new BufferedOutputStream(new GZIPOutputStream(
						new FileOutputStream(file)));
			} else {
				writer = new BufferedOutputStream(new FileOutputStream(file));
			}
		}

		public StandardFileWriter() {
		}

		public void write(Tuple tuple) throws IOException {
			if (tuple.getNElements() > 0) {
				String value = tuple.get(0).toString();
				for (int i = 1; i < tuple.getNElements(); ++i) {
					value += " " + tuple.get(i).toString();
				}
				value += "\n";
				writer.write(value.getBytes());
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
	public void registerActionParameters(ActionConf conf) {
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

		boolean compressGZip = false;
		if (outputDirectory.endsWith(".gz")) {
			compressGZip = true;
		}

		// Calculate the filename
		File f = new File(outputDirectory);

		if (!f.exists()) {
			f.mkdir();
		}

		String filename = "part-" + nf.format(context.getCounter("OutputFile"))
				+ "_" + nf.format(0);
		if (compressGZip) {
			filename += ".gz";
		}
		f = new File(f, filename);

		if (customWriter != null) {
			Constructor<? extends StandardFileWriter> constr;
			try {
				constr = Class
						.forName(customWriter)
						.asSubclass(StandardFileWriter.class)
						.getConstructor(ActionContext.class, File.class);
			} catch (Throwable e) {
				log.error("Could not load class " + customWriter, e);
				throw new IOException("Could not load class " + customWriter, e);
			}
			try {
				file = constr.newInstance(context, f);
			} catch (InvocationTargetException e) {
				Throwable ex = e.getCause();
				if (ex instanceof IOException) {
					log.error("got IOException", e);
					throw (IOException) ex;
				}
				log.error("Could not instantiate class " + customWriter, e);
				throw new IOException("Could not instantiate", e);
			} catch (Throwable e) {
				log.error("Could not instantiate class " + customWriter, e);
				throw new IOException("Could not instantiate", e);
			}
		} else {
			log.debug("No custom writer is specified. Using standard one");
			if (compressGZip)
				file = new StandardFileWriter(context, f, true);
			else
				file = new StandardFileWriter(context, f, false);
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
