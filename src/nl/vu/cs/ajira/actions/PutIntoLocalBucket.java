package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.data.types.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutIntoLocalBucket extends Action {

	static final Logger log = LoggerFactory.getLogger(PutIntoLocalBucket.class);
	public static final int ALL_NODES = -1;

	public static final int BUCKET_ID = 0;
	public static final String S_BUCKET_ID = "Bucket ID";
	public static final int SORT = 1;
	public static final String S_SORT = "sorting_function";

	Bucket bucket = null;
	int destID, bucketID;
	String sPartitioner;
	boolean sort;

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
		conf.registerParameter(SORT, S_SORT, null, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		bucketID = getParamInt(BUCKET_ID);
		sort = getParamBoolean(SORT);
		bucket = context.getBucket(bucketID, sort, null);
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
