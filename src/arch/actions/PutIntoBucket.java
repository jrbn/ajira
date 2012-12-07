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
	static {
		registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
	}

	Bucket bucket = null;
	int bucketID;

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		bucketID = getParamInt(BUCKET_ID);
		bucket = context.getTuplesBuckets().getOrCreateBucket(
				chain.getSubmissionNode(), chain.getSubmissionId(), bucketID,
				null, null);
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
