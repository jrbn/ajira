package nl.vu.cs.ajira.datalayer.files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class it is used to parse the content of a file.
 * 
 */
public class DefaultFileReader implements FileReader {

	static final Logger log = LoggerFactory.getLogger(DefaultFileReader.class);
	protected BufferedReader reader = null;
	TString currentLine = new TString();

	/**
	 * Custom constructor.
	 * 
	 * @param file
	 *          the file that will be used for reading.
	 * @throws IOException
	 * 			in case of trouble.
	 */
	@Override
	public void init(File file) throws IOException {
		if (log.isDebugEnabled()) {
			log.debug("Reading file " + file.getPath());
		}
		InputStream input = new FileInputStream(file);
		if (file.getName().endsWith(".gz")) {
			input = new GZIPInputStream(input);
		}
		reader = new BufferedReader(new InputStreamReader(input));
	}

	/**
	 * Tries to read one line from the input file, and returns whether
	 * it can be obtained by {@link #getTuple(Tuple)}.
	 * 
	 * @throws IOException
	 * 			in case of trouble.
	 * @return
	 *			<code>true</code> if it was possible to read a line, <code>false</code> otherwise.
	 */
	@Override
	public boolean next() throws IOException {
		String s = reader.readLine();
		if (s == null) {
			try {
				reader.close();
			} catch(Throwable e) {
				log.info("Got exception while closing reader (ignored)", e);
			}
			reader = null;
			currentLine = null;
			return false;
		}
		currentLine.setValue(s);
		return true;
	}

	/**
	 * Updates the field of the tuple with last line read.
	 * 
	 * @param tuple
	 *          the tuple that will be updated with the line.
	 */
	@Override
	public void getTuple(Tuple tuple) {
		if (currentLine == null) {
			throw new NoSuchElementException("No tuple available");
		}
		tuple.set(currentLine);
	}

	@Override
	public void close() {
		try {
			if (reader != null) {
				reader.close();
			}
		} catch (Throwable e) {
			log.info("Got exception while closing reader (ignored)", e);
			// ignore
		}
	}
}
