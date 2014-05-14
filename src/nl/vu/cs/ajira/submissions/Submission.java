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
	private final int finalStatsReceived;
	private String state;
	private Throwable exception;

	Map<String, Long> counters = null;
	int assignedBucket = -1;
	int rootChainsReceived = -1;
	boolean mainRootReceived;

	Map<Long, Integer> monitors = new HashMap<Long, Integer>();

	public Submission(int submissionId, int assignedOutputBucket) {
		startupTime = System.currentTimeMillis();
		state = Consts.STATE_OPEN;
		finalStatsReceived = 0;
		rootChainsReceived = -1;
		assignedBucket = assignedOutputBucket;
		this.submissionId = submissionId;
		mainRootReceived = false;
	}

	public int getId() {
		return submissionId;
	}

	public double getExecutionTimeInMs() {
		return endTime - startupTime;
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

	public long getStartTime() {
		return startupTime;
	}

	public int getSubmissionId() {
		return submissionId;
	}

	public synchronized String getState() {
		return state;
	}

	public synchronized void setState(String state) {
		this.state = state;
		this.notifyAll();
	}

	public void setFinished(String state) {
		setState(state);
		endTime = System.currentTimeMillis();
	}

	@Override
	public String toString() {
		return "Submission-" + submissionId;
	}

	public Map<String, Long> getCounters() {
		return counters;
	}

	public String getStatistics() {
		StringBuffer b = new StringBuffer();
		b.append("\n**************************************************\nOUTPUT OF JOB ");
		b.append(getSubmissionId());
		b.append(":\n-> status: ");
		b.append(getState());
		b.append("\n-> counters: ");

		if (counters != null) {
			b.append("\n");
			for (Map.Entry<String, Long> entry : counters.entrySet()) {
				b.append(" ");
				b.append(entry.getKey());
				b.append(" = ");
				b.append(entry.getValue());
				b.append("\n");
			}
		} else {
			b.append("NONE");
		}
		b.append("\n-> execution time: ");
		b.append(getExecutionTimeInMs());
		b.append(" ms.\n");
		b.append("**************************************************\n");

		return b.toString();
	}

	public void printStatistics() {

		String stats = getStatistics();

		if (log.isInfoEnabled()) {
			log.info(stats);
		} else {
			System.out.println(stats);
		}

		if (getState().equals(Consts.STATE_FAILED) && exception != null) {
			if (log.isErrorEnabled()) {
				log.error("Job failed with exception", exception);
			} else {
				System.err.println("Job failed with exception " + exception);
				exception.printStackTrace(System.err);
			}
		}
	}

	public Throwable getException() {
		return exception;
	}

	public void setException(Throwable exception) {
		this.exception = exception;
	}
}
