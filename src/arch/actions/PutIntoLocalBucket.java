package arch.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.buckets.Bucket;
import arch.data.types.Tuple;

public class PutIntoLocalBucket extends Action {

	static final Logger log = LoggerFactory.getLogger(PutIntoLocalBucket.class);
	public static final int ALL_NODES = -1;

	public static final int BUCKET_ID = 0;
	public static final String S_BUCKET_ID = "Bucket ID";
	public static final int SORTING_FUNCTION = 1;
	public static final String S_SORTING_FUNCTION = "sorting_function";

	Bucket bucket = null;
	int destID, bucketID;
	String sortingFunction, sPartitioner;

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
		conf.registerParameter(SORTING_FUNCTION, S_SORTING_FUNCTION, null,
				false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		bucketID = getParamInt(BUCKET_ID);
		sortingFunction = getParamString(SORTING_FUNCTION);
		bucket = context.getBucket(bucketID, sortingFunction);
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
