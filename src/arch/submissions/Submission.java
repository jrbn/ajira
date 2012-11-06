package arch.submissions;

import java.util.HashMap;
import java.util.Map;

public class Submission {

	long startupTime;
	long endTime;
	int submissionId;
	int finalStatsReceived;
	String state;
	Map<String, Long> counters = null;
	int assignedBucket = -1;
	int rootChainsReceived = -1;

	// boolean printStats;
	// boolean printIntermediateStats;

	Map<Long, int[]> monitors = new HashMap<Long, int[]>();

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

	public void init() {
		monitors.clear();
		assignedBucket = -1;
		rootChainsReceived = -1;
		counters = null;
	}
}