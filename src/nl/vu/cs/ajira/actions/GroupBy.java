package nl.vu.cs.ajira.actions;

import java.util.Iterator;

import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupBy extends Action {

	static final Logger log = LoggerFactory.getLogger(GroupIterator.class);

	private static class GroupIterator implements Iterator<Tuple> {

		private TupleIterator itr;
		private SimpleData[] key;
		private int sizeKey;
		boolean elementAvailable;
		boolean moreGroups;

		private Tuple inputTuple;

		private SimpleData[] sValues;
		private Tuple outputTuple;

		public GroupIterator(TupleIterator itr, Tuple inputTuple,
				SimpleData[] key, int sizeKey) throws Exception {
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

			this.moreGroups = true;

			for (int i = 0; i < sizeKey; ++i) {
				key[i] = DataProvider.getInstance().get(
						inputTuple.get(i).getIdDatatype());
			}
		}

		public void initKey() {

			this.elementAvailable = true;
			// Copy the key elements in the internal key data structure
			for (int i = 0; i < sizeKey; ++i) {
				this.inputTuple.get(i).copyTo(key[i]);
			}
		}

		@Override
		public boolean hasNext() {
			return elementAvailable;
		}

		@Override
		public Tuple next() {
			// Copy the value elements to return
			if (elementAvailable) {
				for (int i = 0; i < sValues.length; ++i) {
					inputTuple.get(sizeKey + i).copyTo(sValues[i]);
				}

				// Move to the next element
				try {

					boolean nextElement = itr.nextTuple();
					moreGroups = nextElement;

					if (nextElement) {
						itr.getTuple(inputTuple);
						for (int i = 0; i < sizeKey && elementAvailable; ++i) {
							elementAvailable = inputTuple.get(i).equals(key[i]);
						}
					} else {
						elementAvailable = false;
					}
				} catch (Exception e) {
					log.error("Error with the iterator", e);
					elementAvailable = false;
					moreGroups = false;
				}
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
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context) {

			// Which fields should be used for the grouping? Get them from the
			// parameter
			ActionConf partition = ActionFactory
					.getActionConf(PartitionToNodes.class);

			partition.setParamBoolean(PartitionToNodes.SORT, true);
			byte[] fieldsToSort = (byte[]) params[FIELDS_TO_GROUP];
			partition.setParamByteArray(PartitionToNodes.SORTING_FIELDS,
					fieldsToSort);
			partition.setParamByteArray(PartitionToNodes.PARTITION_FIELDS,
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
		conf.registerParameter(FIELDS_TO_GROUP, "FIELDS_TO_GROUP", null, true);
		conf.registerParameter(TUPLE_FIELDS, "TUPLE_FIELDS", null, true);
		conf.registerParameter(NPARTITIONS_PER_NODE, "NPARTITIONS_PER_NODE", null,
				false);
		conf.registerCustomConfigurator(new Configurator());
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
			itr.initKey();
			actionOutput.output(outputTuple);
			itr.resetIterator();
		} while (itr.moreGroups);
	}
}
