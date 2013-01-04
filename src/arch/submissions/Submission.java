package arch.submissions;

import java.util.HashMap;
import java.util.Map;

import arch.Context;
import arch.utils.Consts;

public class Submission {
	
	private static final String BUCKETCOUNTER_NAME = "BucketCounter";
	private static final String CHAINCOUNTER_NAME = "ChainCounter";
	
	private static final long BUCKET_INIT = 100;
	private static final long CHAIN_INIT = 100;

	private final long startupTime;
	private long endTime;
	private final int submissionId;
	private int finalStatsReceived;
	private String state;

	Map<String, Long> counters = null;
	int assignedBucket = -1;
	int rootChainsReceived = -1;
	private final String chainCounterName;
	private final String bucketCounterName;
	private final Context context;
	
	// boolean printStats;
	// boolean printIntermediateStats;

	Map<Long, int[]> monitors = new HashMap<Long, int[]>();

	public Submission(int submissionId, int assignedOutputBucket, Context context) {
		startupTime = System.nanoTime();
		state = Consts.STATE_OPEN;
		finalStatsReceived = 0;
		rootChainsReceived = -1;
		assignedBucket = assignedOutputBucket;
		this.submissionId = submissionId;
		chainCounterName = CHAINCOUNTER_NAME + "-" + submissionId;
		bucketCounterName = BUCKETCOUNTER_NAME + "-" + submissionId;
		this.context = context;
		initializeCounter(chainCounterName, CHAIN_INIT);
		initializeCounter(bucketCounterName, BUCKET_INIT);
	}

	public double getExecutionTimeInMs() {
		return (double) (endTime - startupTime) / 1000 / 1000;
	}

	public int getAssignedBucket() {
		return assignedBucket;
	}

	public int getFinalStatsReceived() {
		return finalStatsReceived;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public int getSubmissionId() {
		return submissionId;
	}

	public String getState() {
		return state;
	}
	
	public void setState(String state) {
		this.state = state;
	}

	
	public long getUniqueCounter(String name) {
		 return context.getUniqueCounter(name);
	}
	
	public void initializeCounter(String name, long init) {
		context.initializeCounter(name, init);
	}

	public long getNewChainID() {
		return getUniqueCounter(chainCounterName);
	}

	public int getNewBucketID() {
		return (int) getUniqueCounter(bucketCounterName);
	}

	public void setFinished() {
		setState(Consts.STATE_FINISHED);
		endTime = System.nanoTime();
		context.deleteCounter(chainCounterName);
		context.deleteCounter(bucketCounterName);
	}
}