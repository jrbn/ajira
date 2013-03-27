package nl.vu.cs.ajira.submissions;

import ibis.ipl.WriteMessage;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.buckets.Buckets;
import nl.vu.cs.ajira.chains.Chain;
import nl.vu.cs.ajira.chains.ChainExecutor;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.mgmt.StatisticsCollector;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.storage.Container;
import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.SubmissionCache;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubmissionRegistry {

	static final Logger log = LoggerFactory.getLogger(SubmissionRegistry.class);

	Factory<Chain> chainFactory = new Factory<Chain>(Chain.class);

	ActionFactory ap;
	DataProvider dp;
	StatisticsCollector stats;
	Buckets buckets;
	NetworkLayer net;
	Configuration conf;

	Map<Integer, Submission> submissions = new ConcurrentHashMap<Integer, Submission>();
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
			long parentChainId, int nchildren, long[] additionalChainCounters,
			int[] additionalChainValues) {
		Submission sub = getSubmission(submissionId);

		if (log.isDebugEnabled()) {
			log.debug("updateCounters: submissionId = " + submissionId
					+ ", chainId = " + chainId + ", parentChainId = "
					+ parentChainId + ", nchildren = " + nchildren);
		}
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

			if (additionalChainCounters != null) {
				for (int i = 0; i < additionalChainCounters.length; ++i) {
					Integer c = sub.monitors.get(additionalChainCounters[i]);
					if (c == null) {
						c = additionalChainValues[i];
					} else {
						c += additionalChainValues[i];
					}
					if (c == 0) {
						sub.monitors.remove(additionalChainCounters[i]);
					} else {
						sub.monitors.put(additionalChainCounters[i], c);
					}
				}
			}

			if (parentChainId == -1) { // It is one of the root chains
				sub.rootChainsReceived++;
				if (chainId == 0) {
					sub.mainRootReceived = true;
				}
			} else if (parentChainId >= 0) {
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

			if (log.isDebugEnabled()) {
				log.debug("rootChainsReceived = " + sub.rootChainsReceived
						+ ", mainRootReceived = " + sub.mainRootReceived
						+ ", monitors.size() = " + sub.monitors.size());
			}
			if (sub.rootChainsReceived == 0 && sub.mainRootReceived
					&& sub.monitors.size() == 0) {
				if (sub.assignedBucket != -1) {
					Bucket bucket = buckets.getExistingBucket(submissionId,
							sub.assignedBucket);
					bucket.waitUntilFinished();
				}

				sub.setFinished(Consts.STATE_FINISHED);
				sub.notifyAll();
			}
		}
	}

	public void killSubmission(int submissionId, Throwable e) {
		Submission sub = submissions.get(submissionId);
		if (sub == null) {
			return;
		}
		synchronized (sub) {
			if (sub.getState() == Consts.STATE_FINISHED) {
				return;
			}
			sub.setFinished(Consts.STATE_FINISHED);
			sub.setException(e);
			sub.notifyAll();
		}
	}

	public Submission getSubmission(int submissionId) {
		return submissions.get(submissionId);
	}

	private Submission submitNewJob(Context context, Job job) {

		int submissionId;
		synchronized (this) {
			submissionId = submissionCounter++;
		}
		Submission sub = new Submission(submissionId, -1);

		try {
			submissions.put(submissionId, sub);
			
			ActionConf[] actions = job.getActions();

			if (actions == null) {
				throw new Exception("No action is defined!");
			}

			Chain chain = new Chain();
			chain.setParentChainId(-1);
			chain.setInputLayer(Consts.DEFAULT_INPUT_LAYER_ID);
			int resultBucket = chain.setActions(
					new ChainExecutor(null, context, chain), actions);

			if (resultBucket != -1) {
				sub.assignedBucket = resultBucket;
			}
			chain.setSubmissionNode(context.getNetworkLayer().getMyPartition());
			chain.setSubmissionId(submissionId);

			// If local
			if (context.isLocalMode()) {
				chainsToProcess.add(chain);
			} else {
				context.getNetworkLayer().sendChain(chain);
			}

		} catch (Exception e) {
			log.error("Init of the submission " + sub + " has failed", e);
			submissions.remove(submissionId);
			sub.setFinished(Consts.STATE_INIT_FAILED);
		}

		return sub;
	}

	public void releaseSubmission(Submission submission) {
		submissions.remove(submission.getSubmissionId());
	}

	public void setState(int submissionId, String state) {
		submissions.get(submissionId).setState(state);
	}

	public void cleanupSubmission(Submission submission) {
		try {
			for (int i = 0; i < net.getNumberNodes(); ++i) {
				if (i == net.getMyPartition()) {
					cache.clearAll(submission.getSubmissionId());
				} else {
					WriteMessage msg = net.getMessageToSend(
							net.getPeerLocation(i),
							NetworkLayer.nameMgmtReceiverPort);
					msg.writeByte((byte) 8);
					msg.writeInt(submission.getSubmissionId());
					msg.finish();
				}
			}
		} catch (Exception e) {
			log.error("Failure in cleaning up the submission", e);
		}
	}

	public Submission waitForCompletion(Context context, Job job)
			throws JobFailedException {

		Submission submission;
		try {
			submission = submitNewJob(context, job);
		} catch (Throwable e1) {
			throw new JobFailedException("Job submission failed", e1);
		}

		// Pool the submission registry to know when it is present and return it
		synchronized (submission) {
			while (!submission.getState().equalsIgnoreCase(
					Consts.STATE_FINISHED)
					&& !submission.getState().equalsIgnoreCase(
							Consts.STATE_INIT_FAILED)) {
				try {
					submission.wait(1000);
				} catch (InterruptedException e) {
					// ignore
				}
			}
		}

		// Clean up the eventual things going on in the nodes
		try {
			cleanupSubmission(submission);
		} catch (Throwable e) {
			// TODO: what to do here?
			// Should probably not affect the job.
		}
		Throwable e = submission.getException();
		if (e != null) {
			throw new JobFailedException(e);
		}
		return submission;
	}

	public void getStatistics(Submission submission) {
		try {
			Thread.sleep(Consts.STATISTICS_COLLECTION_INTERVAL);
		} catch (InterruptedException e) {
			// ignore
		}
		submission.counters = stats.removeCountersSubmission(submission
				.getSubmissionId());
	}

	public Collection<Submission> getAllSubmissions() {
		return submissions.values();
	}
}
