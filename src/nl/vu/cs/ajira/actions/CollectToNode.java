package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectToNode extends Action {

	static final Logger log = LoggerFactory.getLogger(CollectToNode.class);

	/* PARAMETERS */
	public static final int I_NODE_ID = 0;
	private static final int I_BUCKET_ID = 1;
	public static final int B_SORT = 2;
	public static final int IA_SORTING_FIELDS = 3;
	public static final int SA_TUPLE_FIELDS = 4;
	public static final int B_STREAMING_MODE = 5;

	private int nodeId;
	private int bucketId = -1;
	private boolean sort;
	private byte[] sortingFields;
	private byte[] fields;
	private Bucket bucket;
	private boolean streamingMode;

	private static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context) {
			if (params[I_NODE_ID] == null) {
				params[I_NODE_ID] = context.getMyNodeId();
			}
			params[I_BUCKET_ID] = context.getNewBucketID();
			controller.continueComputationOn(
					((Integer) params[I_NODE_ID]).intValue(),
					(Integer) params[I_BUCKET_ID]);

			// Convert the tuple fields in numbers
			TStringArray fields = (TStringArray) params[SA_TUPLE_FIELDS];
			byte[] f = new byte[fields.getArray().length];
			int i = 0;
			for (String v : fields.getArray()) {
				f[i++] = (byte) DataProvider.getId(v);
			}
			params[SA_TUPLE_FIELDS] = f;
			params[IA_SORTING_FIELDS] = convertToBytes(params[IA_SORTING_FIELDS]);
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_NODE_ID, "I_NODE_ID", null, false);
		conf.registerParameter(I_BUCKET_ID, "I_BUCKET_ID", null, false);
		conf.registerParameter(B_SORT, "B_SORT", false, false);
		conf.registerParameter(IA_SORTING_FIELDS, "IA_SORTING_FIELDS", null,
				false);
		conf.registerParameter(SA_TUPLE_FIELDS, "SA_TUPLE_FIELDS", null, true);
		conf.registerParameter(B_STREAMING_MODE, "B_STREAMING_MODE", false,
				false);
		conf.registerCustomConfigurator(new ParametersProcessor());
	}

	@Override
	public void startProcess(ActionContext context) {
		nodeId = getParamInt(I_NODE_ID);
		bucketId = getParamInt(I_BUCKET_ID);
		sort = getParamBoolean(B_SORT);
		sortingFields = getParamByteArray(IA_SORTING_FIELDS);
		fields = getParamByteArray(SA_TUPLE_FIELDS);
		streamingMode = getParamBoolean(B_STREAMING_MODE);
		bucket = null;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (bucket == null) {
			bucket = context.startTransfer(nodeId, bucketId, sort,
					sortingFields, fields, streamingMode);
		}
		bucket.add(inputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		context.finishTransfer(nodeId, bucketId, sort, sortingFields,
				bucket != null, fields, streamingMode);
		bucket = null;
	}
}
