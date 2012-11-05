package arch.datalayer.files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.data.types.TString;
import arch.data.types.Tuple;

public class LineTextFilesReader extends FileIterator {

    static final Logger log = LoggerFactory
	    .getLogger(LineTextFilesReader.class);
    BufferedReader reader = null;
    TString currentLine = new TString();

    public LineTextFilesReader(File file) {
	super(file);
	try {
	    log.debug("Reading file " + file.getPath());
	    InputStream input = new FileInputStream(file);
	    if (file.getName().endsWith(".gz")) {
		input = new GZIPInputStream(input);
	    }
	    reader = new BufferedReader(new InputStreamReader(input));
	} catch (Exception e) {
	    log.error("Failed reading file " + file);
	}
    }

    @Override
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

    @Override
    public void getTuple(Tuple tuple) throws Exception {
	tuple.set(currentLine);
    }
}
