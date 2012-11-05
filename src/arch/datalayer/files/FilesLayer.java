package arch.datalayer.files;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.datalayer.TupleIterator;
import arch.storage.Factory;

public class FilesLayer extends InputLayer {

	public final static int OP_LS = 0;
	public final static int OP_READ = 1;

	public final static String IMPL_FILE_READER = "fileslayer.reader.impl";

	static final Logger log = LoggerFactory.getLogger(FilesLayer.class);

	Factory<TString> factory = new Factory<TString>(TString.class);
	Factory<TInt> factoryInt = new Factory<TInt>(TInt.class);
	int numberNodes;
	int currentPivot;

	Class<? extends FileIterator> classFileIterator = null;

	@Override
	protected void load(Context context) throws IOException {
		currentPivot = -1;
		numberNodes = context.getNetworkLayer().getNumberNodes();

		String clazz = context.getConfiguration().get(IMPL_FILE_READER, null);
		try {
			classFileIterator = Class.forName(clazz).asSubclass(
					FileIterator.class);
		} catch (Exception e) {
			log.error("Failed in loading the file reader class", e);
		}

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
				itr = new ListFilesReader(value.getValue(), sFilter);
			} else { // OP_READ

				// In this case value is the key to a file collection
				FileCollection col = (FileCollection) context
						.getObjectFromCache(value.getValue());
				if (col == null) {
					// Get it remotely. Operation will contain the node id.
					tuple.get(operation, 2);
					List<Object[]> files = context
							.retrieveRemoteCacheObjects(value.getValue());
					if (files == null || files.size() == 0) {
						throw new Exception("Failed retrieving the iterator");
					}
					col = (FileCollection) files.get(0)[0];
				}

				itr = new MultiFilesReader(col, classFileIterator);
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
	public int[] getLocations(Tuple tuple, Chain chain, Context context) {
		int[] range = new int[2];
		if (++currentPivot == numberNodes) {
			currentPivot = 0;
		}

		range[0] = range[1] = currentPivot;
		return range;
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
	}

}
