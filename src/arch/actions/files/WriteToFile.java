package arch.actions.files;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.NumberFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.chains.Chain;
import arch.data.types.DataProvider;
import arch.data.types.SimpleData;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class WriteToFile extends Action {

	final static Logger log = LoggerFactory.getLogger(WriteToFile.class);

	static public class StandardFileWriter {

		FileWriter writer = null;
		DataProvider dp = null;
		SimpleData[] array = null;

		public StandardFileWriter(ActionContext context, File file)
				throws IOException {
			writer = new FileWriter(file);
			dp = context.getDataProvider();
		}

		public StandardFileWriter() {
		}

		public void write(Tuple tuple) throws Exception {
			if (array == null) {
				array = new SimpleData[tuple.getNElements()];
				for (int i = 0; i < tuple.getNElements(); ++i) {
					array[i] = dp.get(tuple.getType(i));
				}
			}

			tuple.get(array);
			String value = array[0].toString();
			for (int i = 1; i < array.length; ++i) {
				value += " " + array[i].toString();
			}
			writer.write(value + "\n");
		}

		public void close() throws IOException {
			writer.close();
		}
	}

	StandardFileWriter file = null;
	String outputDirectory = null;
	String customWriter = null;

	public void setOutputDirectory(String outputDirectory) {
		this.outputDirectory = outputDirectory;
	}

	public void setCustomWriter(Class<? extends StandardFileWriter> clazz) {
		customWriter = clazz.getName();
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int l = input.readByte();
		byte[] content = new byte[l];
		input.readFully(content);
		outputDirectory = new String(content);

		l = input.readByte();
		if (l > 0) {
			content = new byte[l];
			input.readFully(content);
			customWriter = new String(content);
		} else {
			customWriter = null;
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		byte[] b = outputDirectory.getBytes();
		output.writeByte(b.length);
		output.write(b);
		if (customWriter == null) {
			output.writeByte(0);
		} else {
			b = customWriter.getBytes();
			output.writeByte(b.length);
			output.write(b);
		}
	}

	@Override
	public int bytesToStore() throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public void startProcess(ActionContext context, Chain chain) {
		file = null;
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

		f = new File(f, "part-"
				+ nf.format(context.getUniqueCounter("OutputFile")) + "_"
				+ nf.format(0));

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
	public void process(ActionContext context, Chain chain,
			Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess)
			throws Exception {
		if (file == null) {
			openFile(context);
		}
		file.write(inputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToSend) throws Exception {
		if (file != null) {
			file.close();
		}
		file = null;
	}

}
