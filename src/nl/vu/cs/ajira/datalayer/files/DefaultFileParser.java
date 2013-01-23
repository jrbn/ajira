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


public class DefaultFileParser {

	static final Logger log = LoggerFactory
			.getLogger(DefaultFileParser.class);
	protected BufferedReader reader = null;
	TString currentLine = new TString();

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

	public boolean next() {
		try {
			currentLine.setValue(reader.readLine());
			if (currentLine.getValue() == null) {
				reader.close();
				return false;
			}
			return true;
		} catch (Exception e) {
			log.error("Error reading record", e);
		}
		return false;
	}

	public void getTuple(Tuple tuple) throws Exception {
		tuple.set(currentLine);
	}
}
