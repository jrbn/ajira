package nl.vu.cs.ajira.datalayer.files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
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
public class DefaultFileParser {

	static final Logger log = LoggerFactory
			.getLogger(DefaultFileParser.class);
	protected BufferedReader reader = null;
	TString currentLine = new TString();

	/**
	 * Custom constructor.
	 * 
	 * @param file
	 * 		The file that will be used for reading.
	 */
	public DefaultFileParser(File file) {
		try {
			if (log.isDebugEnabled()) {
				log.debug("Reading file " + file.getPath());
			}
			InputStream input = new FileInputStream(file);
			if (file.getName().endsWith(".gz")) {
				input = new GZIPInputStream(input);
			}
			reader = new BufferedReader(new InputStreamReader(input));
		} catch (Exception e) {
			log.error("Failed reading file " + file);
		}
	}

	/**
	 * Reads one line from the input file.
	 * 
	 * @return
	 * 		True if it was possible to read a line, false otherwise.
	 */
	public boolean next() {
		try {
			String s = reader.readLine();
			if (s == null) {
				reader.close();
				return false;
			}
			currentLine.setValue(s);
			return true;
		} catch (Exception e) {
			log.error("Error reading record", e);
		}
		return false;
	}

	/**
	 * Updates the field of the tuple with last line read.
	 * 
	 * @param tuple
	 * 		The tuple that will be updated with the 
	 * 		current line read.
	 * 		
	 * @throws Exception
	 */
	public void getTuple(Tuple tuple) throws Exception {
		tuple.set(currentLine);
	}
}
