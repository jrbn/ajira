package arch.submissions;

import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.Context;
import arch.actions.ActionFactory;
import arch.buckets.Bucket;
import arch.buckets.Buckets;
import arch.chains.ActionsExecutor;
import arch.chains.Chain;
import arch.data.types.DataProvider;
import arch.net.NetworkLayer;
import arch.statistics.StatisticsCollector;
import arch.storage.Container;
import arch.storage.Factory;
import arch.storage.SubmissionCache;
import arch.utils.Configuration;
import arch.utils.Consts;

public class SubmissionRegistry {

	static final Logger log = LoggerFactory.getLogger(SubmissionRegistry.class);

	Factory<Chain> chainFactory = new Factory<Chain>(Chain.class);
	Factory<Submission> submissionFactory = new Factory<Submission>(
			Submission.class);

	ActionFactory ap;
	DataProvider dp;
	StatisticsCollector stats;
	Buckets buckets;
	NetworkLayer net;
	Configuration conf;

	Map<Integer, Submission> submissions = new HashMap<Integer, Submission>();
	Container<Chain> chainsToProcess;
	SubmissionCache cache;
	int submissionCounter = 0;

	public SubmissionRegistry(NetworkLayer net, StatisticsCollector stats,
			Container<Chain> chainsToProcess, Buckets buckets,
			ActionFactory ap, DataProvider dp, SubmissionCache cache,
			Configuration conf) {
		this.net = net;
		this.stats = stats;
		this.chainsToProcess = chainsToProcess;
		this.buckets = buckets;
		this.ap = ap;
		this.dp = dp;
		this.conf = conf;
		this.cache = cache;
	}

	public void updateCounters(int submissionId, long chainId,
			long parentChainId, int nchildren) {
		Submission sub = getSubmission(submissionId);

		synchronized (sub) {
			if (nchildren > 0) { // Set the expected children in the
				// map
				Integer c = sub.monitors.get(chainId);
				if (c == null) {
					c = nchildren;
				} else {
					c += nchildren;
				}
				if (c == 0) {
					sub.monitors.remove(chainId);
				} else {
					sub.monitors.put(chainId, c);
				}
			}

			if (parentChainId == -1) { // It is one of the root chains
				sub.rootChainsReceived = 0;
			} else {
				// Change the children field of the parent chain
				Integer c = sub.monitors.get(parentChainId);
				if (c == null) {
					sub.monitors.put(parentChainId, -1);
				} else {
					c--;
					if (c == 0) {
						sub.monitors.remove(parentChainId);
					} else {
						sub.monitors.put(parentChainId, c);
					}
				}
			}

			if (sub.rootChainsReceived == 0 && sub.monitors.size() == 0) {
				if (sub.assignedBucket != -1) {
					Bucket bucket = buckets.getExistingBucket(submissionId,
							sub.assignedBucket);
					bucket.waitUntilFinished();
				}

				sub.state = Consts.STATE_FINISHED;
				sub.endTime = System.nanoTime();
				sub.notifyAll();
			}
		}
	}

	public Submission getSubmission(int submissionId) {
		return submissions.get(submissionId);
	}

	private Submission submitNewJob(Context context, Job job) throws Exception {

		Chain chain = new Chain();
		chain.setParentChainId(-1);
		chain.setInputLayer(Consts.DEFAULT_INPUT_LAYER_ID);
		chain.addActions(job.getActions(), new ActionsExecutor(context, null,
				chain));

		Submission sub = submissionFactory.get();
		sub.init();
		sub.startupTime = System.nanoTime();
		synchronized (this) {
			sub.submissionId = submissionCounter++;
		}
		sub.state = Consts.STATE_OPEN;
		sub.finalStatsReceived = 0;
		sub.rootChainsReceived = -1;
		sub.assignedBucket = job.getAssignedOutputBucket();

		submissions.put(sub.submissionId, sub);
		chain.setSubmissionNode(context.getNetworkLayer().getMyPartition());
		chain.setSubmissionId(sub.submissionId);

		// If local
		if (context.isLocalMode()) {
			chainsToProcess.add(chain);
		} else {
			context.getNetworkLayer().sendChain(chain);
		}

		return sub;
	}

	public void releaseSubmission(Submission submission) {
		submissions.remove(submission.submissionId);
		submissionFactory.release(submission);
	}

	public void setState(int submissionId, String state) {
		submissions.get(submissionId).state = state;
	}

	public void cleanupSubmission(Submission submission) throws IOException,
			InterruptedException {

		for (int i = 0; i < net.getNumberNodes(); ++i) {
			if (i == net.getMyPartition()) {
				cache.clearAll(submission.getSubmissionId());
			} else {
				WriteMessage msg = net.getMessageToSend(net.getPeerLocation(i),
						NetworkLayer.nameMgmtReceiverPort);
				msg.writeByte((byte) 8);
				msg.writeInt(submission.getSubmissionId());
				msg.finish();
			}
		}
	}

	public Submission waitForCompletion(Context context, Job job)
			throws Exception {

		Submission submission = submitNewJob(context, job);
		int waitInterval = conf.getInt(Consts.STATISTICAL_INTERVAL,
				Consts.DEFAULT_STATISTICAL_INTERVAL);
		// Pool the submission registry to know when it is present and return it
		synchronized (submission) {
			while (!submission.getState().equalsIgnoreCase(
					Consts.STATE_FINISHED)) {
				submission.wait(waitInterval);
				// if (submission.printIntermediateStats
				// && !submission.getState().equalsIgnoreCase(
				// Consts.STATE_FINISHED))
				// stats.printStatistics(submission.getSubmissionId());
			}
		}

		// Clean up the eventual things going on in the nodes
		cleanupSubmission(submission);
		return submission;
	}

	public void getStatistics(Job job, Submission submission)
			throws InterruptedException {
		// if (job.getWaitForStatistics()) {
		// try {
		// log.info("Waiting for statistics...");
		Thread.sleep(500);
		// } catch (InterruptedException e) {
		// // ignore
		// }
		// }

		submission.counters = stats.removeCountersSubmission(submission
				.getSubmissionId());

		// Print the counters
		// if (submission.printStats) {
		String stats = "Final statistics for job "
				+ submission.getSubmissionId() + ":\n";
		if (submission.counters != null) {
			for (Map.Entry<String, Long> entry : submission.counters.entrySet()) {
				stats += " " + entry.getKey() + " = " + entry.getValue() + "\n";
			}
		}
		log.info(stats);
		// System.out.println(stats);
		// }
	}
}
