package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.Query;
import nl.vu.cs.ajira.storage.TupleComparator;

public class GroupBy extends Action {

	public static int FIELDS_TO_GROUP = 0;

	public static class Configurator extends ActionConf.Configurator {

		@Override
		void setupConfiguration(Query query, Object[] params,
				ActionController controller, ActionContext context)
				throws Exception {

			// Which fields should be used for the grouping? Get them from the
			// parameter
			byte[] fieldsToSort = (byte[]) params[0];
			ActionConf partition = ActionFactory
					.getActionConf(PartitionToNodes.class);
			partition.setParam(PartitionToNodes.SORTING_FUNCTION,
					TupleComparator.class);
			if (fieldsToSort != null)
				partition.setParam(PartitionToNodes.SORTING_FIELDS,
						fieldsToSort);
			controller.addAction(partition);
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(FIELDS_TO_GROUP, "fieldsToGroup", null, true);
		conf.registerCustomConfigurator(Configurator.class);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		// TODO Auto-generated method stub

	}

}
