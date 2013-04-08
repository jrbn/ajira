package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;

public class Project extends Action {

	public static final int BA_FIELDS = 0;

	private SimpleData[] output;
	private byte[] fields;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(BA_FIELDS, "fields to keep", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		fields = getParamByteArray(BA_FIELDS);
		output = new SimpleData[fields.length];
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		for (int i = 0; i < fields.length; ++i) {
			output[i] = tuple.get(fields[i]);
		}
		actionOutput.output(output);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		fields = null;
		output = null;
	}
}
