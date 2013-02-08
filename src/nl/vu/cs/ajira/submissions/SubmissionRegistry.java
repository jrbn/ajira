package nl.vu.cs.ajira.submissions;

import ibis.ipl.WriteMessage;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.buckets.Buckets;
import nl.vu.cs.ajira.chains.Chain;
import nl.vu.cs.ajira.chains.ChainExecutor;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.statistics.StatisticsCollector;
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
			long parentChainId, int nchildren, int generatedRootChains) {
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

			if (generatedRootChains > 0) {
				sub.rootChainsReceived -= generatedRootChains;
			}

			if (parentChainId == -1) { // It is one of the root chains
				sub.rootChainsReceived++;
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

				sub.setFinished();
				sub.notifyAll();
			}
		}
	}
	
	public void killSubmission(int submissionId, Throwable e) {
	    Submission sub = submissions.get(submissionId);
	    if (sub == null) {
		return;
	    }
	    synchronized(sub) {
		if (sub.getState() == Consts.STATE_FINISHED) {
		    return;
		}
		sub.setFinished();
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
		Submission sub = new Submission(submissionId,
				job.getAssignedOutputBucket());

		try {

			submissions.put(submissionId, sub);

			Chain chain = new Chain();
			chain.setParentChainId(-1);
			chain.setInputLayer(Consts.DEFAULT_INPUT_LAYER_ID);
			chain.addActions(job.getActions(), new ChainExecutor(context, null,
					chain));

			chain.setSubmissionNode(context.getNetworkLayer().getMyPartition());
			chain.setSubmissionId(submissionId);

			// If local
			if (context.isLocalMode()) {
				chainsToProcess.add(chain);
			} else {
				context.getNetworkLayer().sendChain(chain);
			}

		} catch (Exception e) {
			log.error("Init of the job " + job + " has failed");
			submissions.remove(submissionId);
			sub.setState(Consts.STATE_INIT_FAILED);
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

	public Submission waitForCompletion(Context context, Job job) throws JobFailedException {

		Submission submission;
		try {
		    submission = submitNewJob(context, job);
		} catch (Throwable e1) {
		    throw new JobFailedException("Job submission failed", e1);
		}
		int waitInterval = conf.getInt(Consts.STATISTICAL_INTERVAL,
				Consts.DEFAULT_STATISTICAL_INTERVAL);
		// Pool the submission registry to know when it is present and return it
		synchronized (submission) {
			while (!submission.getState().equalsIgnoreCase(
					Consts.STATE_FINISHED)) {
			    try {
				submission.wait(waitInterval);
			    } catch(Throwable e) {
				// ignore
			    }
			}
		}

		// Clean up the eventual things going on in the nodes
		try {
		    cleanupSubmission(submission);
		} catch(Throwable e) {
		    // TODO: what to do here?
		    // Should probably not affect the job.
		}
		Throwable e = submission.getException();
		if (e != null) {
		    throw new JobFailedException(e);
		}
		return submission;
	}

	public void getStatistics(Job job, Submission submission) {
		try {
		    Thread.sleep(500);
		} catch (InterruptedException e) {
		    // ignore
		}

		submission.counters = stats.removeCountersSubmission(submission
				.getSubmissionId());

		// Print the counters
		String stats = "Final statistics for job "
				+ submission.getSubmissionId() + ":\n";
		if (submission.counters != null) {
			for (Map.Entry<String, Long> entry : submission.counters.entrySet()) {
				stats += " " + entry.getKey() + " = " + entry.getValue() + "\n";
			}
		}
		log.info(stats);
	}
}
