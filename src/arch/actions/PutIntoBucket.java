package arch.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.buckets.Bucket;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class PutIntoBucket extends Action {

	static final Logger log = LoggerFactory.getLogger(PutIntoBucket.class);

	public static final int BUCKET_ID = 0;
	public static final String S_BUCKET_ID = "Bucket ID";
	public static final int SORTING_FUNCTION = 1;
	public static final String S_SORTING_FUNCTION = "sorting_function";

	Bucket bucket = null;
	int bucketID;
	String sortingFunction;

	@Override
	public void setupActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
		conf.registerParameter(SORTING_FUNCTION, S_SORTING_FUNCTION, null,
				false);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		bucketID = getParamInt(BUCKET_ID);
		sortingFunction = getParamString(SORTING_FUNCTION);

		bucket = context.getBuckets().getOrCreateBucket(
				chain.getSubmissionNode(), chain.getSubmissionId(), bucketID,
				sortingFunction, null);
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception {
		bucket.add(inputTuple);
		output.add(inputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToSend) throws Exception {
		bucket.setFinished(true);
	}
}
