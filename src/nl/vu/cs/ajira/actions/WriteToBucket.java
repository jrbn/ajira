package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>WriteToBucket</code> action stores its input tuples in a specific
 * bucket, as well as passing them on to the specified {@link ActionOutput}.
 */
public class WriteToBucket extends Action {

	static final Logger log = LoggerFactory.getLogger(WriteToBucket.class);

	/**
	 * The <code>I_BUCKET_ID</code> parameter is of type <code>int</code>, is
	 * required, and specifies the bucket number of the destination bucket.
	 */
	public static final int I_BUCKET_ID = 0;

	/**
	 * The <code>SA_TUPLE_FIELDS</code> parameter is of type
	 * <code>String[]</code>, is required, and specifies the class name of the
	 * type of each field in the tuple (see {@link nl.vu.cs.ajira.data.types}).
	 */
	public static final int SA_TUPLE_FIELDS = 1;

	private Bucket bucket = null;
	private int bucketID;
	private byte[] fields;

	private static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context) {
			// Convert the tuple fields in numbers
			TStringArray fields = (TStringArray) params[SA_TUPLE_FIELDS];
			byte[] f = new byte[fields.getArray().length];
			int i = 0;
			for (String v : fields.getArray()) {
				f[i++] = (byte) DataProvider.getId(v);
			}
			params[SA_TUPLE_FIELDS] = f;
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_BUCKET_ID, "I_BUCKET_ID", null, true);
		conf.registerParameter(SA_TUPLE_FIELDS, "SA_TUPLE_FIELDS", null, true);
		conf.registerCustomConfigurator(new ParametersProcessor());
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		bucketID = getParamInt(I_BUCKET_ID);
		fields = getParamByteArray(SA_TUPLE_FIELDS);
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
		bucket.setFinished();
	}
}
