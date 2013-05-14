package nl.vu.cs.ajira.datalayer.files;

import java.io.IOException;
import java.util.List;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.Location;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class provides an iterator for different types of files, it a layer over
 * the files.
 * 
 */
public class FileLayer extends InputLayer {

	public final static int OP_LS = 0;
	public final static int OP_READ = 1;
	private final static Class<? extends DefaultFileReader> DEFAULT_FILE_PARSER = DefaultFileReader.class;

	static final Logger log = LoggerFactory.getLogger(FileLayer.class);

	int numberNodes;

	/**
	 * Sets the number of nodes that are used by the current context.
	 */
	@Override
	protected void load(Context context) throws IOException {
		numberNodes = context.getNetworkLayer().getNumberNodes();
	}

	/**
	 * Returns a Iterator. Depending on the operation given through the tuple
	 * parameter it can be a ListFilesIterator or a FilesIterator.
	 */
	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		try {
			int operation = ((TInt) tuple.get(0)).getValue();
			String value = ((TString) tuple.get(1)).getValue();

			TupleIterator itr = null;
			if (operation == OP_LS) {
				// Read if there is also a filter
				String sFilter = null;
				if (tuple.getNElements() == 3) {
					sFilter = ((TString) tuple.get(2)).getValue();
				}
				itr = new ListFilesIterator(value, sFilter);
			} else { // OP_READ

				// In this case value is the key to a file collection
				FileCollection col = (FileCollection) context
						.getObjectFromCache(value);
				if (col == null) {
					// Get it remotely. Operation will contain the node id.
					operation = ((TInt) tuple.get(2)).getValue();
					List<Object[]> files = context.retrieveCacheObjects(value);
					if (files == null || files.size() == 0) {
						throw new Exception("Failed retrieving the iterator");
					}
					for (int i = 0; i < files.size() && col == null; ++i) {
						col = (FileCollection) files.get(i)[0];
					}
				}

				if (tuple.getNElements() == 4) {
					// There is a customized file reader
					String clazz = ((TString) tuple.get(3)).getValue();

					// Test whether it is a good class
					Class<? extends FileReader> c = null;
					try {
						c = Class.forName(clazz).asSubclass(FileReader.class);
					} catch (Exception e) {
						log.warn("Customized file parser " + clazz
								+ " is not valid.");
					}
					if (c != null)
						itr = new FilesIterator(col, c);
					else
						itr = new FilesIterator(col, DEFAULT_FILE_PARSER);

				} else {
					itr = new FilesIterator(col, DEFAULT_FILE_PARSER);
				}

			}

			itr.init(context, "Files");
			return itr;
		} catch (Exception e) {
			log.error("Unable getting tuple iterator", e);
		}

		return null;
	}

	/**
	 * Depending on the first value of the tuple it returns the ChainLocation of
	 * this node or a new ChainLocation whose id is formed from a substring of
	 * the second value of the tuple.
	 */
	@Override
	public Location getLocations(Tuple tuple, ActionContext context) {
		try {
			if (((TInt) tuple.get(0)).getValue() == OP_LS) {
				return Location.THIS_NODE;
			} else {
				String s = ((TString) tuple.get(1)).getValue();
				int index = Integer.valueOf(s.substring(s.indexOf('-') + 1));
				return new Location(index % numberNodes);
			}

		} catch (Exception e) {
			log.error("Error", e);
		}
		return null;
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
	}
}
