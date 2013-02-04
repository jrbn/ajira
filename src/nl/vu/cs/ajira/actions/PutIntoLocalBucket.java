package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.data.types.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutIntoLocalBucket extends Action {

	static final Logger log = LoggerFactory.getLogger(PutIntoLocalBucket.class);
	public static final int ALL_NODES = -1;

	public static final int BUCKET_ID = 0;
	private static final String S_BUCKET_ID = "Bucket ID";
	public static final int TUPLE_FIELDS = 1;
	private static final String S_TUPLE_FIELDS = "tuple_fields";

	private Bucket bucket = null;
	private int bucketID;
	private byte[] fields;

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
		conf.registerParameter(TUPLE_FIELDS, S_TUPLE_FIELDS, null, true);
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
