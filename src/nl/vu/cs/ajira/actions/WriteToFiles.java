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

/**
 * The <code>WriteToFiles</code> action writes its input to a file, by means of a
 * so-called file-writer. A default file-writer is provided, whose functionality is
 * to write to a file, possibly compressed.
 * The file itself is placed in a user-specified output directory.
 */
public class WriteToFiles extends Action {

	final static Logger log = LoggerFactory.getLogger(WriteToFiles.class);

	/**
	 * The <code>S_CUSTOM_WRITER</code> parameter, of type <code>String</code>,
	 * is not required, and defaults to the class name of {@link StandardFileWriter}.
	 * When supplied, it should indicate a class name of a class that must extend
	 * {@link StandardFileWriter}, and must have a public constructor with two parameters:
	 * an {@link ActionContext}, and a {@link file}.
	 */
	public static final int S_CUSTOM_WRITER = 0;
	
	/**
	 * The <code>S_OUTPUT_DIR</code> parameter, of type <code>String</code>,
	 * is required, and specifies the directory where the output file(s) are stored.
	 * It should either be a directory, or not exist yet.
	 */
	public static final int S_OUTPUT_DIR = 1;

	/**
	 * The <code>StandardFileWriter</code> is the default file writer.
	 */
	static public class StandardFileWriter {

		protected BufferedOutputStream writer = null;

		/**
		 * Constructs a <code>StandardFileWriter</code> for the specified file. It
		 * creates a stream to write to, possibly compressed, as specified by the
		 * <code>compressGZip</code> parameter.
		 * 
		 * @param context
		 * 		the action context of this action
		 * @param file
		 * 		the file to write to
		 * @param compressGZip
		 * 		whether to compress or not
		 * @throws IOException
		 * 		is thrown when the file could not be opened for some reason
		 */
		public StandardFileWriter(ActionContext context, File file,
				boolean compressGZip) throws IOException {
			if (compressGZip) {
				writer = new BufferedOutputStream(new GZIPOutputStream(
						new FileOutputStream(file)));
			} else {
				writer = new BufferedOutputStream(new FileOutputStream(file));
			}
		}

		/**
		 * Constructor for when this class is sub-classed.
		 */
		public StandardFileWriter() {
		}

		/**
		 * This method writes the specified tuple to the output stream, by
		 * first constructing a string from the tuple (fields of the tuple separated
		 * by a space) and then writing that string to the output stream.
		 * @param tuple
		 * 		the tuple to write
		 * @throws IOException
		 * 		is thrown on write error
		 */
		public void write(Tuple tuple) throws IOException {
			if (tuple.getNElements() > 0) {
				StringBuilder b = new StringBuilder(tuple.get(0).toString());
				for (int i = 1; i < tuple.getNElements(); ++i) {
					b.append(' ').append(tuple.get(i).toString());
				}
				b.append('\n');
				writer.write(b.toString().getBytes());
			}
		}

		/**
		 * Closes the output stream.
		 * @throws IOException
		 * 		is thrown when closing the underlying output stream throws it.
		 */
		public void close() throws IOException {
			writer.close();
		}
	}

	private StandardFileWriter file = null;
	private String outputDirectory = null;
	private String customWriter = null;
	private long count;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_CUSTOM_WRITER, "S_CUSTOM_WRITER", null, false);
		conf.registerParameter(S_OUTPUT_DIR, "S_OUTPUT_DIR", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		outputDirectory = getParamString(S_OUTPUT_DIR);
		customWriter = getParamString(S_CUSTOM_WRITER);
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
			f.mkdirs();
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
