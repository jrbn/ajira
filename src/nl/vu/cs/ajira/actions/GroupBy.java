package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.Query;
import nl.vu.cs.ajira.datalayer.TupleIterator;

public class GroupBy extends Action {

	public static int FIELDS_TO_GROUP = 0;
	public static final int TUPLE_FIELDS = 1;

	private TupleIterator itr;
	private byte[] fieldsToGroup;

	public static class Configurator extends ActionConf.Configurator {

		@Override
		void setupConfiguration(Query query, Object[] params,
				ActionController controller, ActionContext context)
				throws Exception {

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
			controller.addAction(partition);

			controller.doNotAddCurrentAction();
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(FIELDS_TO_GROUP, "fieldsToGroup", null, true);
		conf.registerParameter(TUPLE_FIELDS, "fields", null, true);
		conf.registerCustomConfigurator(Configurator.class);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		// Get the tuple iterator
		fieldsToGroup = getParamByteArray(FIELDS_TO_GROUP);
		itr = context.getInputIterator();
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		// TODO Auto-generated method stub

	}

}
