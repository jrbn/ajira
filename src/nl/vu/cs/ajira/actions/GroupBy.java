package nl.vu.cs.ajira.actions;

import java.util.Iterator;

import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.Query;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupBy extends Action {

	private static class GroupIterator implements Iterator<Tuple> {

		static final Logger log = LoggerFactory.getLogger(GroupIterator.class);

		private TupleIterator itr;
		private SimpleData[] key;
		private int sizeKey;
		boolean elementAvailable;
		boolean hasMore;

		private Tuple inputTuple;

		private SimpleData[] sValues;
		private Tuple outputTuple;

		public GroupIterator(TupleIterator itr, Tuple inputTuple,
				SimpleData[] key, int sizeKey) {
			this.itr = itr;
			this.key = key;
			this.sizeKey = sizeKey;
			this.inputTuple = inputTuple;

			// The values are a copy of the current tuple
			this.sValues = new SimpleData[inputTuple.getNElements() - sizeKey];
			for (int i = 0; i < sValues.length; ++i) {
				sValues[i] = DataProvider.getInstance().get(
						inputTuple.get(sizeKey + i).getIdDatatype());
			}
			this.outputTuple = TupleFactory.newTuple(sValues);

			this.elementAvailable = true;
			this.hasMore = true;

			// Copy the key elements in the internal key data structure
			for (int i = 0; i < sizeKey; ++i) {
				key[i] = DataProvider.getInstance().get(
						inputTuple.get(i).getIdDatatype());
				inputTuple.get(i).copyTo(key[i]);
			}
		}

		@Override
		public boolean hasNext() {
			return elementAvailable;
		}

		@Override
		public Tuple next() {
			// Copy the value elements to return
			for (int i = 0; i < sValues.length; ++i) {
				inputTuple.get(sizeKey + i).copyTo(sValues[i]);
			}

			// Move to the next element
			try {
				if (itr.nextTuple()) {

					itr.getTuple(inputTuple);
					for (int i = 0; i < sizeKey && elementAvailable; ++i) {
						elementAvailable = inputTuple.get(i).equals(key[i]);
					}

					// Copy key
					for (int i = 0; i < sizeKey; ++i) {
						inputTuple.get(i).copyTo(key[i]);
					}

					hasMore = true;
				} else {
					elementAvailable = false;
					hasMore = false;
				}
			} catch (Exception e) {
				log.error("Error with the iterator", e);
				elementAvailable = false;
			}

			return outputTuple;
		}

		@Override
		public void remove() {
		}

		public void resetIterator() {
			elementAvailable = true;
		}
	}

	public static int FIELDS_TO_GROUP = 0;
	public static final int TUPLE_FIELDS = 1;
	public static final int NPARTITIONS_PER_NODE = 2;

	private GroupIterator itr;
	private byte[] posFieldsToGroup;
	private SimpleData[] outputTuple;

	public static class Configurator extends ActionConf.Configurator {

		@Override
		void setupAction(Query query, Object[] params,
				ActionController controller, ActionContext context) {

			// Which fields should be used for the grouping? Get them from the
			// parameter
			ActionConf partition = ActionFactory
					.getActionConf(PartitionToNodes.class);

			partition.setParamBoolean(PartitionToNodes.SORT, true);
			byte[] fieldsToSort = (byte[]) params[FIELDS_TO_GROUP];
			partition.setParamByteArray(PartitionToNodes.SORTING_FIELDS,
					fieldsToSort);
			partition.setParamStringArray(PartitionToNodes.TUPLE_FIELDS,
					(TStringArray) params[TUPLE_FIELDS]);
			if (params[NPARTITIONS_PER_NODE] != null) {
				partition.setParamInt(PartitionToNodes.NPARTITIONS_PER_NODE,
						(Integer) params[NPARTITIONS_PER_NODE]);
			}
			controller.addAction(partition);
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(FIELDS_TO_GROUP, "fieldsToGroup", null, true);
		conf.registerParameter(TUPLE_FIELDS, "fields", null, true);
		conf.registerParameter(NPARTITIONS_PER_NODE, "partitionsPerNode", null,
				false);
		conf.registerCustomConfigurator(Configurator.class);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		posFieldsToGroup = getParamByteArray(FIELDS_TO_GROUP);
		outputTuple = new SimpleData[posFieldsToGroup.length + 1];
		itr = null;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		itr = new GroupIterator(context.getInputIterator(), tuple, outputTuple,
				posFieldsToGroup.length);
		outputTuple[posFieldsToGroup.length] = new TBag(itr);

		do {
			actionOutput.output(outputTuple);
			itr.resetIterator();
		} while (itr.hasMore);
	}
}
