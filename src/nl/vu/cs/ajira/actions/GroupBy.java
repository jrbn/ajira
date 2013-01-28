package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.Query;

public class GroupBy extends Action {

	public static int FIELDS_TO_GROUP = 0;

	public static class Configurator extends ActionConf.Configurator {

		@Override
		void setupConfiguration(Query query, Object[] params,
				ActionController controller, ActionContext context)
				throws Exception {

			// Which fields should be used for the grouping? Get them from the
			// parameter
			if (params[0] instanceof Integer) {
				// TODO: single field.
			} else {
				// Array of positions
			}

			// First partition the triples across the nodes
			ActionConf action = ActionFactory
					.getActionConf(PartitionToNodes.class);
			controller.addAction(action);

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
