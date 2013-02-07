package nl.vu.cs.ajira.submissions;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.utils.Consts;


public class Submission {

	private final long startupTime;
	private long endTime;
	private final int submissionId;
	private int finalStatsReceived;
	private String state;
	private Throwable exception;

	Map<String, Long> counters = null;
	int assignedBucket = -1;
	int rootChainsReceived = -1;

	Map<Long, Integer> monitors = new HashMap<Long, Integer>();

	public Submission(int submissionId, int assignedOutputBucket) {
		startupTime = System.nanoTime();
		state = Consts.STATE_OPEN;
		finalStatsReceived = 0;
		rootChainsReceived = -1;
		assignedBucket = assignedOutputBucket;
		this.submissionId = submissionId;
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

	public void setFinished() {
		setState(Consts.STATE_FINISHED);
		endTime = System.nanoTime();
	}
	
	public Throwable getException() {
	    return exception;
	}

	public void setException(Throwable exception) {
	    this.exception = exception;
	}
}
