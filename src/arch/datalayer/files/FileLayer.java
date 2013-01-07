package arch.datalayer.files;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.Context;
import arch.actions.ActionContext;
import arch.chains.ChainLocation;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.datalayer.TupleIterator;
import arch.storage.Factory;

public class FileLayer extends InputLayer {

	public final static int OP_LS = 0;
	public final static int OP_READ = 1;
	private final static Class<? extends DefaultFileParser> DEFAULT_FILE_PARSER = DefaultFileParser.class;

	static final Logger log = LoggerFactory.getLogger(FileLayer.class);

	Factory<TString> factory = new Factory<TString>(TString.class);
	Factory<TInt> factoryInt = new Factory<TInt>(TInt.class);
	int numberNodes;

	@Override
	protected void load(Context context) throws IOException {
		numberNodes = context.getNetworkLayer().getNumberNodes();
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		try {
			TInt operation = factoryInt.get();
			TString value = factory.get();
			tuple.get(operation, value);

			// Read if there is also a filter
			String sFilter = null;
			if (tuple.getNElements() == 3) {
				TString filter = factory.get();
				tuple.get(filter, 2);
				sFilter = filter.getValue();
				factory.release(filter);
			}

			TupleIterator itr = null;
			if (operation.getValue() == OP_LS) {
				itr = new ListFilesIterator(value.getValue(), sFilter);
			} else { // OP_READ

				// In this case value is the key to a file collection
				FileCollection col = (FileCollection) context
						.getObjectFromCache(value.getValue());
				if (col == null) {
					// Get it remotely. Operation will contain the node id.
					tuple.get(operation, 2);
					List<Object[]> files = context
							.retrieveCacheObjects(value.getValue());
					if (files == null || files.size() == 0) {
						throw new Exception("Failed retrieving the iterator");
					}
					for (int i = 0; i < files.size() && col == null; ++i) {
						col = (FileCollection) files.get(i)[0];
					}
				}

				if (tuple.getNElements() == 4) {
					// There is a customized file reader
					TString clazz = factory.get();
					tuple.get(clazz, 3);

					// Test whether it is a good class
					Class<? extends DefaultFileParser> c = null;
					try {
						c = Class.forName(clazz.getValue()).asSubclass(
								DefaultFileParser.class);
					} catch (Exception e) {
						log.warn("Customized file parser " + clazz.getValue()
								+ " is not valid.");
					}
					if (c != null)
						itr = new FilesIterator(col, c);
					else
						itr = new FilesIterator(col, DEFAULT_FILE_PARSER);
					factory.release(clazz);

				} else {
					itr = new FilesIterator(col, DEFAULT_FILE_PARSER);
				}

			}

			factoryInt.release(operation);
			factory.release(value);
			return itr;
		} catch (Exception e) {
			log.error("Unable getting tuple iterator", e);
		}

		return null;
	}

	@Override
	public ChainLocation getLocations(Tuple tuple, ActionContext context) {
		TInt operation = factoryInt.get();
		TString value = factory.get();
		try {
			tuple.get(operation, value);
			if (operation.getValue() == OP_LS) {
				return ChainLocation.THIS_NODE;
			} else {
                                String s = value.toString();
				int index = Integer.valueOf(s.substring(s.indexOf('-')+1));
				return new ChainLocation(index % numberNodes);
			}

		} catch (Exception e) {
			log.error("Error", e);
		} finally {
			factoryInt.release(operation);
			factory.release(value);
		}
		return null;
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
	}

}
