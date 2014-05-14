package nl.vu.cs.ajira.net;

import ibis.ipl.MessageUpcall;
import ibis.ipl.ReadMessage;
import ibis.ipl.SendPortIdentifier;
import ibis.ipl.WriteMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.buckets.Buckets;
import nl.vu.cs.ajira.buckets.WritableTuple;
import nl.vu.cs.ajira.chains.Chain;
import nl.vu.cs.ajira.exceptions.JobFailedException;
import nl.vu.cs.ajira.mgmt.StatisticsCollector;
import nl.vu.cs.ajira.storage.Container;
import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to implement the receive side protocol of a participant
 * node. The main role is to make sure that the send-receive protocol is
 * maintained correctly while the tuples are being transfered.
 */
class Receiver implements MessageUpcall {
	static final Logger log = LoggerFactory.getLogger(Receiver.class);

	Factory<Chain> chainFactory = new Factory<Chain>(Chain.class);
	Factory<WritableContainer<WritableTuple>> bufferFactory;

	Context context;
	Container<Chain> chainsToProcess;
	Buckets buckets;
	NetworkLayer net;
	StatisticsCollector stats;

	int myId;

	/**
	 * Receiver's custom constructor.
	 * 
	 * @param context
	 *            Current context
	 * @param bufferFactory
	 *            The factory used for generating buffers (buffer allocation and
	 *            memory management)
	 */
	public Receiver(Context context,
			Factory<WritableContainer<WritableTuple>> bufferFactory) {
		this.context = context;
		this.chainsToProcess = context.getChainHandlerManager()
				.getChainsToProcess();
		this.buckets = context.getBuckets();
		this.net = context.getNetworkLayer();
		this.stats = context.getStatisticsCollector();
		this.bufferFactory = bufferFactory;
		myId = net.getMyPartition();

	}

	/**
	 * This method is used for decoding the message and performing the actions
	 * prescribed by the id field. When a message arrives, it gets disassembled
	 * and, based on its specified id (command id) and the rest of the fields,
	 * we continue or stop the protocol (acts like a finite state machine).
	 */
	@Override
	public void upcall(ReadMessage message) throws IOException,
			ClassNotFoundException {
		long time = System.currentTimeMillis();
		byte messageId = message.readByte();
		SendPortIdentifier origin = message.origin();

		WriteMessage reply;

		if (log.isDebugEnabled()) {
			Thread.currentThread().setName(
					"Upcall " + message.origin().ibisIdentifier() + ", id = "
							+ messageId);
			log.debug("Starting upcall");
		}

		switch (messageId) {
		case 0: // Chain to process
			Chain chain = chainFactory.get();
			chain.readFrom(new ReadMessageWrapper(message));
			endMessage(message, time, chain.getSubmissionId(), false);
			if (log.isDebugEnabled()) {
				log.debug("Received chain " + chain.getChainId());
			}
			chainsToProcess.add(chain);
			chainFactory.release(chain);
			break;
		case 1: // Receive signal that remote node has some tuples to send to
			// local bucket or that a chain didn't have.
			int submissionNode = message.readInt();
			int idSubmission = message.readInt();
			int idBucket = message.readInt();

			boolean updateCounters = message.readBoolean();

			long idChain = 0,
			idParentChain = 0;
			int children = 0;
			boolean isResponsible = false;
			if (updateCounters) {
				idChain = message.readLong();
				idParentChain = message.readLong();
				children = message.readInt();
				isResponsible = message.readBoolean();
			}

			boolean isSorted = message.readBoolean();
			byte[] cfParams = null;
			if (isSorted) {
				int lengthSortingParams = message.readInt();
				if (lengthSortingParams > 0) {
					cfParams = new byte[lengthSortingParams];
					message.readArray(cfParams);
				}
			}
			long bufferKey = message.readLong();
			boolean streaming = message.readBoolean();
			int lSignature = message.readByte();
			byte[] signature = new byte[lSignature];
			message.readArray(signature);

			int additionalCounters = message.readByte();
			long[] chains = null;
			int[][] vals = null;
			if (additionalCounters > 0) {
				chains = new long[additionalCounters];
				vals = new int[additionalCounters][];
				for (int i = 0; i < additionalCounters; ++i) {
					chains[i] = message.readLong();
					int[] v = new int[message.readInt()];
					for (int j = 0; j < v.length; ++j) {
						v[j] = message.readByte();
					}
					vals[i] = v;
				}
			}
			endMessage(message, time, idSubmission, true);

			Bucket bucket = buckets.getOrCreateBucket(submissionNode,
					idSubmission, idBucket, isSorted, isSorted, streaming,
					cfParams, signature);
			if (updateCounters) {
				bucket.updateCounters(idChain, idParentChain, children,
						isResponsible);
			}

			if (additionalCounters > 0) {
				bucket.setAdditionalCounters(chains, vals);
			}

			if (bufferKey > -1) {
				int idRemoteNode = net.getPeerId(message.origin()
						.ibisIdentifier());
				net.signalsBucketToFetch(idSubmission, submissionNode,
						idBucket, idRemoteNode, bufferKey, streaming);
			} else {
				if (bufferKey == -1)
					bucket.updateCounters(0, true);
			}

			break;
		case 2: // Signal chain is terminated
			boolean isChainFailed = message.readBoolean();
			idSubmission = message.readInt();

			if (!isChainFailed) {
				idChain = message.readLong();
				idParentChain = message.readLong();
				children = message.readInt();

				long[] additionalC = null;
				int[] additionalCV = null;
				int s = message.readInt();
				if (s > 0) {
					additionalC = new long[s];
					additionalCV = new int[s];
					for (int i = 0; i < s; ++i) {
						additionalC[i] = message.readLong();
						additionalCV[i] = message.readInt();
					}
				}

				endMessage(message, time, idSubmission, true);
				context.getSubmissionsRegistry().updateCounters(idSubmission,
						idChain, idParentChain, children, additionalC,
						additionalCV);
			} else {
				// Cleanup submission
				submissionNode = message.readInt();
				Throwable e = (Throwable) message.readObject();
				endMessage(message, time, idSubmission, true);
				context.cleanupSubmission(submissionNode, idSubmission, e);
				buckets.removeBucketsOfSubmission(idSubmission);
			}
			break;
		case 3: // Termination
			try {
				context.getNetworkLayer().stopIbis();
			} catch (Throwable e) {
				log.error("Got exception in stopIbis()", e);
			}
			System.exit(0);
			break;
		case 4: // Request to transfer a bucket
			long bucketKey = message.readLong();
			int submissionId = message.readInt();
			submissionNode = message.readInt();
			int bucketId = message.readInt();
			long ticket = message.readLong();
			int sequence = message.readInt();
			int nrequest = message.readInt();
			endMessage(message, time, submissionId, false);
			net.addRequestToSendTuples(bucketKey,
					net.getPeerId(message.origin().ibisIdentifier()),
					submissionId, submissionNode, bucketId, ticket, sequence,
					nrequest);
			break;
		case 5: // A bucket to copy to local
			ticket = message.readLong();
			submissionId = message.readInt();
			submissionNode = message.readInt();
			bucketId = message.readInt();
			sequence = message.readInt();
			bucketKey = message.readLong();
			nrequest = message.readInt();
			boolean isSortingBucket = message.readBoolean();
			isSorted = message.readBoolean();
			boolean data = message.readBoolean();
			if (data) {
                                boolean compressed = message.readBoolean();
				net.removeActiveRequest(ticket);

				WritableContainer<WritableTuple> container = bufferFactory
						.get();
				container.init(isSortingBucket);

                                if (compressed) {
                                    int s = message.readInt();
                                    byte[] compressed_size = new byte[s];
                                    message.readArray(compressed_size, 0, s);
                                    container.decompressFrom(compressed_size, s);
                                } else {
                                    container.readFrom(new ReadMessageWrapper(message));
                                }

				boolean isFinished = message.readBoolean();
				endMessage(message, time, submissionId, true);
				// Finish message before calling getExistingBucket, because that
				// call may block.
				bucket = buckets.getExistingBucket(submissionId, bucketId);
				bucket.addAll(container, isSorted);
				// addAll takes over control of container.
				bucket.updateCounters(sequence, isFinished);

				if (!isFinished) {
					// Need to request for another fetch
					int remoteNodeId = net.getPeerId(origin.ibisIdentifier());
					net.signalsBucketToFetch(submissionId, submissionNode,
							bucketId, remoteNodeId, bucketKey, ++sequence,
							++nrequest);
				}
			} else {
				endMessage(message, time, submissionId, true);
				int remoteNodeId = net.getPeerId(origin.ibisIdentifier());
				// Resubmit it
				net.signalsBucketToFetch(submissionId, submissionNode,
						bucketId, remoteNodeId, bucketKey, sequence, ++nrequest);
			}
			break;
		case 6: // Statistics Collector
			while (message.readByte() == 1) {
				submissionId = message.readInt();
				int nParams = message.readInt();
				while (nParams-- > 0) {
					int lengthString = message.readInt();
					byte[] list = new byte[lengthString];
					message.readArray(list);
					long value = message.readLong();
					stats.addCounter(myId, submissionId, new String(list),
							value);
				}
			}
			/*
			 * No need to finish message. Upcall returns. message.finish(); if
			 * (log.isDebugEnabled()) { log.debug("Finished upcall, t = " +
			 * (System.currentTimeMillis() - time)); }
			 */
			break;
		case 7:
			// New job to submit
			Job job = new Job();
			job.readFrom(new ReadMessageWrapper(message));
			message.finish();
			net.startMonitorCounters();
			net.broadcastStartMonitoring();

			Submission submission = context.getSubmissionsRegistry()
					.waitForCompletion(context, job);

			net.stopMonitorCounters();
			net.broadcastStopMonitoring();

			reply = net.getMessageToSend(message.origin().ibisIdentifier(),
					NetworkLayer.queryReceiverPort);

			if (submission.getState().equals(Consts.STATE_FINISHED)) {
				int bid = submission.getAssignedBucket();
				boolean[] retval = new boolean[1];

				WritableContainer<WritableTuple> tmpBuffer;

				reply.writeBoolean(true); // Success

				reply.writeDouble(submission.getExecutionTimeInMs());

				bucket = null;

				if (bid >= 0) {
					// Read the bucket
					bucket = buckets.getExistingBucket(
							submission.getSubmissionId(),
							submission.getAssignedBucket());
					tmpBuffer = bucket.removeWChunk(retval);
				} else {
					retval[0] = true;
					tmpBuffer = bufferFactory.get();
				}
				// Write a reply to the origin containing the results of this
				// submission
				tmpBuffer.writeTo(new WriteMessageWrapper(reply));

				if (bucket != null) {
					byte[] sign = bucket.getSignature();
					reply.writeInt(sign.length);
					for (int i = 0; i < sign.length; ++i) {
						reply.writeByte(sign[i]);
					}
				} else {
					reply.writeInt(0);
				}

				reply.writeBoolean(retval[0]);
				if (!retval[0]) {
					reply.writeLong(bucket.getKey());
				}
				reply.finish();
				tmpBuffer.clear();
				bufferFactory.release(tmpBuffer);
				tmpBuffer = null;
				context.getSubmissionsRegistry().getStatistics(submission);
				if (job.getPrintStatistics()) {
					submission.printStatistics();
				}

				if (retval[0]) {
					buckets.removeBucketsOfSubmission(submission
							.getSubmissionId());
					Runtime.getRuntime().gc();
				}
				context.getSubmissionsRegistry().releaseSubmission(submission);
			} else {
				reply.writeBoolean(false); // Failure
				// Marshal the exception.
				ByteArrayOutputStream bo = new ByteArrayOutputStream();
				ObjectOutputStream o = new ObjectOutputStream(bo);
				Throwable e = submission.getException();
				if (!(e instanceof JobFailedException)) {
					e = new JobFailedException(
							"Job failed with exception " + e, e);
				}
				o.writeObject(e);
				o.flush();
				o.close();
				byte[] buf = bo.toByteArray();
				reply.writeInt(buf.length);
				reply.writeArray(buf);
				reply.finish();
				if (log.isDebugEnabled()) {
					log.debug("Sent failure reply");
				}
			}
			break;
		case 8:
			// Statistics request
			submissionId = message.readInt();
			message.finish();
			stats.sendStatisticsAway();
			if (myId != 0) {
				// Node 0 has the answer, so cannot clear yet.
				buckets.removeBucketsOfSubmission(submissionId);
				context.getSubmissionCache().clearAll(submissionId);
				Runtime.getRuntime().gc();
			} else {
				// No GC yet, because that intervenes with the connection setup
				// to deliver the answer.
				context.getSubmissionCache().clearAll(submissionId);
			}
			break;

		case 9: // Return results from a large buffer
			bucketKey = message.readLong();
			message.finish();

			boolean[] retval = new boolean[1];
			bucket = buckets.getExistingBucket(bucketKey, true);
			WritableContainer<WritableTuple> tmpBuffer = bucket
					.removeWChunk(retval);

			reply = net.getMessageToSend(message.origin().ibisIdentifier(),
					NetworkLayer.queryReceiverPort);
			tmpBuffer.writeTo(new WriteMessageWrapper(reply));

			byte[] sign = bucket.getSignature();
			reply.writeInt(sign.length);
			for (int i = 0; i < sign.length; ++i) {
				reply.writeByte(sign[i]);
			}

			reply.writeBoolean(retval[0]);
			reply.finish();

			if (log.isDebugEnabled()) {
				log.debug("Sent reply isFinished=" + retval[0] + " tmpBuffer="
						+ tmpBuffer.getNElements());
			}
			bucket.releaseContainer(tmpBuffer);
			tmpBuffer = null;
			if (retval[0]) {
				buckets.removeBucketsOfSubmission((int) (bucketKey >> 32));
				Runtime.getRuntime().gc();
			}
			break;
		case 10: // Broadcast object
			sequence = message.readInt();
			submissionId = message.readInt();
			Object[] keys = (Object[]) message.readObject();
			Object[] values = (Object[]) message.readObject();
			message.finish();

			// Put the obj in the cache
			for (int i = 0; i < keys.length; i++) {
				context.getSubmissionCache().putObjectInCache(submissionId,
						keys[i], values[i]);
			}

			// Return value
			WriteMessage msg = net.getMessageToSend(message.origin()
					.ibisIdentifier());
			msg.writeByte((byte) 11);
			msg.writeInt(sequence);
			msg.finish();
			break;
		case 11: // Acknowledge object is broadcasted
			sequence = message.readInt();
			message.finish();

			NetworkLayer.CountInfo remaining;
			synchronized (net.activeBroadcasts) {
				remaining = net.activeBroadcasts.get(sequence);
			}
			synchronized (remaining) {
				remaining.count--;
				if (remaining.count == 0) {
					remaining.notify();
				}
			}
			break;
		case 12: // Request to send object
			sequence = message.readInt();
			submissionId = message.readInt();
			keys = (Object[]) message.readObject();
			message.finish();

			// Read the objects from the cache
			values = new Object[keys.length];
			for (int i = 0; i < keys.length; i++) {
				values[i] = context.getSubmissionCache().getObjectFromCache(
						submissionId, keys[i]);
				if (values[i] == null) {
					values[i] = SerializableNull.instance;
				}
			}

			// Return value
			msg = net.getMessageToSend(message.origin().ibisIdentifier(),
					NetworkLayer.nameBcstReceiverPort);
			msg.writeByte((byte) 13);
			msg.writeInt(sequence);
			msg.writeObject(values);
			msg.finish();

			break;
		case 13: // Receive objects
			sequence = message.readInt();
			values = (Object[]) message.readObject();
			message.finish();

			NetworkLayer.CountInfo r;
			synchronized (net.activeRetrievals) {
				r = net.activeRetrievals.get(sequence);
			}

			// Remove the null values
			for (int i = 0; i < values.length; ++i) {
				if (values[i] instanceof SerializableNull)
					values[i] = null;
			}

			synchronized (r) {
				r.count--;
				r.receivedObjects.add(values);
				if (r.count == 0) {
					r.notify();
				}
			}
			break;
		case 14: // Request to execute custom code on every node
			// sequence = message.readInt();
			// int nodeId = message.readInt();
			// submissionId = message.readInt();
			// String code = message.readString();
			// message.finish();
			//
			// boolean response = true;
			// try {
			// @SuppressWarnings("unchecked")
			// Class<? extends RemoteCodeExecutor> clazz = (Class<? extends
			// RemoteCodeExecutor>) Class
			// .forName(code);
			// RemoteCodeExecutor rm = clazz.newInstance();
			// ActionContext ac = new ActionContext(context, nodeId,
			// submissionId);
			// rm.execute(ac);
			// } catch (Exception e) {
			// log.error("Failed in running the code", e);
			// response = false;
			// }

			// Return value
			// msg = net.getMessageToSend(message.origin().ibisIdentifier());
			// msg.writeByte((byte) 15);
			// msg.writeInt(sequence);
			// msg.writeBoolean(response);
			// msg.finish();
			break;
		case 15: // Acknowledgment of the execution
			// sequence = message.readInt();
			// boolean response = message.readBoolean();
			// message.finish();
			//
			// // Update the counter
			// NetworkLayer.CountInfo counter;
			// synchronized (net.activeCodeExecutions) {
			// counter = net.activeCodeExecutions.get(sequence);
			// }
			//
			// synchronized (counter) {
			// counter.count--;
			// counter.success &= response;
			// if (counter.count == 0) {
			// counter.notify();
			// }
			// }

			break;
		case 16:
			net.startMonitorCounters();
			break;
		case 17:
			net.stopMonitorCounters();
			break;
		}

		if (log.isDebugEnabled()) {
			log.debug("Ended upcall, t = "
					+ (System.currentTimeMillis() - time));
		}
	}

	/**
	 * Updates the counters when a remote-bucket fetch has finished (basically,
	 * when the signal alert arrives). After that, the local-bucket will be
	 * flagged as being finished.
	 * 
	 * @param msg
	 *            Last message being received
	 * @param startTime
	 *            The last message's send-receive start time
	 * @param submissionId
	 *            The submission id
	 * @param mustFinish
	 *            Set if the message must be finished
	 * @throws IOException
	 */
	public void endMessage(ReadMessage msg, long startTime, int submissionId,
			boolean mustFinish) throws IOException {
		long bytes = mustFinish ? msg.finish() : msg.bytesRead();
		long time = System.currentTimeMillis() - startTime;
		stats.addCounter(0, submissionId,
				"Receiver:upcall: time receiving msgs (ms)", time);
		stats.addCounter(0, submissionId, "Receiver:upcall: bytes received",
				bytes);
		// stats.addCounter(0, submissionId, "Messages read", 1);
	}
}
