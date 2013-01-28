package nl.vu.cs.ajira.submissions;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Submission {

	static final Logger log = LoggerFactory.getLogger(Submission.class);

	private final long startupTime;
	private long endTime;
	private final int submissionId;
	private int finalStatsReceived;
	private String state;

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

	public void setFinished(String state) {
		setState(state);
		endTime = System.nanoTime();
	}

	public void printStatistics() {

		String stats = "\n**************************************************\nOUTPUT OF JOB "
				+ getSubmissionId()
				+ ":\n-> status: "
				+ getState()
				+ "\n-> counters: ";

		if (counters != null) {
			stats += "\n";
			for (Map.Entry<String, Long> entry : counters.entrySet()) {
				stats += " " + entry.getKey() + " = " + entry.getValue() + "\n";
			}
		} else {
			stats += "NONE";
		}
		stats += "\n-> execution time: " + getExecutionTimeInMs()
				+ " ms.\n**************************************************\n";

		System.out.println(stats);
	}
}
