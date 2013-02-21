package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteToBucket extends Action {

	static final Logger log = LoggerFactory.getLogger(WriteToBucket.class);
	public static final int ALL_NODES = -1;

	public static final int BUCKET_ID = 0;
	private static final String S_BUCKET_ID = "Bucket ID";
	public static final int TUPLE_FIELDS = 1;
	private static final String S_TUPLE_FIELDS = "tuple_fields";

	private Bucket bucket = null;
	private int bucketID;
	private byte[] fields;

	static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(Query query, Object[] params,
				ActionController controller, ActionContext context) {
			// Convert the tuple fields in numbers
			TStringArray fields = (TStringArray) params[TUPLE_FIELDS];
			byte[] f = new byte[fields.getArray().length];
			int i = 0;
			for (String v : fields.getArray()) {
				f[i++] = (byte) DataProvider.getId(v);
			}
			params[TUPLE_FIELDS] = f;
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
		conf.registerParameter(TUPLE_FIELDS, S_TUPLE_FIELDS, null, true);
		conf.registerCustomConfigurator(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		bucketID = getParamInt(BUCKET_ID);
		fields = getParamByteArray(TUPLE_FIELDS);
		bucket = context.getBucket(bucketID, false, null, fields);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		bucket.add(inputTuple);
		output.output(inputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		bucket.setFinished(true);
	}
}
