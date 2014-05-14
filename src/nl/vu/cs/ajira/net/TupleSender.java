package nl.vu.cs.ajira.net;

import ibis.ipl.WriteMessage;
import ibis.util.ThreadPool;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.buckets.Buckets;
import nl.vu.cs.ajira.buckets.WritableTuple;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.iq80.snappy.Snappy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to process the incoming requests for a tuple transfer and
 * to answer them by sending chunks from the assigned/specified remote-bucket.
 */
class TupleSender {
	private static final Logger log = LoggerFactory
			.getLogger(TupleSender.class);

	private final Context context;
	private final Buckets buckets;

	private final List<TupleInfo> checkList = new LinkedList<TupleInfo>();
	private final List<TupleInfo> sendList = new LinkedList<TupleInfo>();
	private int checkerTime = 1;
        private final boolean compressing;

	/**
	 * Custom constructor.
	 * 
	 * @param context
	 *            Current context
	 */
	public TupleSender(Context context) {
		this.context = context;
		this.buckets = context.getBuckets();
                this.compressing = context.getConfiguration().getBoolean(Consts.SEND_TUPLES_COMPRESSED, true);
		ThreadPool.createNew(new Runnable() {
			@Override
			public void run() {
				checkTuples();
			}
		}, "TupleSenderChecker");
		for (int i = 0; i < Consts.MAX_TUPLE_SENDERS; i++) {
			ThreadPool.createNew(new Runnable() {
				@Override
				public void run() {
					sendTuples();
				}
			}, "TupleSender " + i);
		}
	}

	/**
	 * Handles new incoming request for data fetch (tuples transfer from the
	 * specified remote-bucket)
	 * 
	 * INFO: This method should be called from a thread that may block.
	 * 
	 * @param localBufferKey
	 *            Local buffer's key (the responsible remote bucket's id)
	 * @param remoteNodeId
	 *            Remote node's id -- the one that requested the transfer
	 * @param idSubmission
	 *            Submission id
	 * @param idBucket
	 *            Bucket id (remote-bucket's id)
	 * @param ticket
	 *            The request's ticket number / id
	 * @param sequence
	 *            Sequence number (~ chunk number)
	 * @param nrequest
	 *            Requests number (how many requests were sent inside this
	 *            message)
	 */
	public void handleNewRequest(long localBufferKey, int remoteNodeId,
			int idSubmission, int submissionNode, int idBucket, long ticket,
			int sequence, int nrequest) {
		final TupleInfo tu = new TupleInfo();

		tu.bucketKey = localBufferKey;
		tu.remoteNodeId = remoteNodeId;
		tu.submissionId = idSubmission;
		tu.submissionNode = submissionNode;
		tu.bucketId = idBucket;
		tu.ticket = ticket;
		tu.sequence = sequence;
		tu.nrequests = nrequest;
		tu.expected = -1;

		Bucket bucket = buckets.getExistingBucket(tu.bucketKey, false);

		if (bucket != null) {
			boolean enoughData = bucket.availableToTransmit()
					|| !buckets.isActiveTransfer(tu.submissionId,
							tu.remoteNodeId, tu.bucketId);
			if (log.isDebugEnabled()) {
				log.debug("Checking bucket "
						+ tu.bucketKey
						+ ", enoughData = "
						+ enoughData
						+ ", isActive = "
						+ buckets.isActiveTransfer(tu.submissionId,
								tu.remoteNodeId, tu.bucketId));
			}
			if (enoughData) {
				synchronized (sendList) {
					sendList.add(tu);
					sendList.notify();
				}

				return;
			}
		}

		synchronized (checkList) {
			checkList.add(tu);
			checkerTime = 1;
			checkList.notify();
		}
	}

	/**
	 * Checks each request to see if it can be answered to. The condition is
	 * that the bucket should have available data for transfer.
	 */
	private void checkTuples() {
		for (;;) {
			synchronized (checkList) {
				while (checkList.size() == 0) {
					try {
						checkList.wait();
					} catch (InterruptedException e) {
						// nothing
					}
				}

				int sz = checkList.size();

				if (log.isDebugEnabled()) {
					log.debug("checkTuples: size = " + sz);
				}

				for (int i = 0; i < sz; i++) {
					TupleInfo info = checkList.remove(0);
					if (context.hasCrashed(info.submissionId)) {
						continue;
					}
					Bucket bucket = buckets.getExistingBucket(info.bucketKey,
							false);
					if (bucket != null) {
						boolean enoughData = bucket.availableToTransmit()
								|| !buckets.isActiveTransfer(info.submissionId,
										info.remoteNodeId, info.bucketId);
						if (enoughData) {
							if (log.isDebugEnabled()) {
								log.debug("Data available for "
										+ context.getNetworkLayer()
												.getPeerLocation(
														info.remoteNodeId)
										+ ", bucket = "
										+ info.bucketKey
										+ ", remote bucket = "
										+ info.bucketId
										+ ", isActive = "
										+ buckets.isActiveTransfer(
												info.submissionId,
												info.remoteNodeId,
												info.bucketId));
							}

							synchronized (sendList) {
								sendList.add(info);
								sendList.notify();
							}

							continue;
						}
					}

					checkList.add(info);
				}

				try {
					checkList.wait(checkerTime);
				} catch (InterruptedException e) {
					// ignore
				}

				checkerTime = Math.min(10, 2 * checkerTime);
			}
		}
	}

	/**
	 * Removes a request from the queue and answers to it.
	 */
	private void sendTuples() {
		byte[] buffer = new byte[10 + Snappy
				.maxCompressedLength(Consts.TUPLES_CONTAINER_MAX_BUFFER_SIZE)];
		for (;;) {
			TupleInfo info;
			synchronized (sendList) {
				while (sendList.size() == 0) {
					try {
						sendList.wait();
					} catch (InterruptedException e) {
						// nothing
					}
				}

				info = sendList.remove(0);
			}
			try {
				sendTuple(info, buffer);
			} catch (Throwable e) {
				if (log.isDebugEnabled()) {
					log.debug("Got Exception in tuple sender", e);
				}
				context.killSubmission(info.submissionNode, info.submissionId,
						e);
			}
		}
	}

	/**
	 * Sends a chunk of tuples as a response to the request for data fetch. We
	 * consider the provided information attached to the request for filling up
	 * the response's message.
	 * 
	 * @param info
	 *            Information about the request
	 * @throws Exception
	 */
	private void sendTuple(TupleInfo info, byte[] supportBuffer)
			throws Exception {

		if (context.hasCrashed(info.submissionId)) {
			return;
		}
		NetworkLayer net = context.getNetworkLayer();
		// long time = System.currentTimeMillis();
		WritableContainer<WritableTuple> tmpBuffer;
		Bucket bucket = buckets.getExistingBucket(info.bucketKey, false);
		if (log.isDebugEnabled()) {
			log.debug("Getting chunk for "
					+ net.getPeerLocation(info.remoteNodeId) + ", bucket = "
					+ info.bucketKey + ", remote bucket = " + info.bucketId);
		}
		tmpBuffer = bucket.nonBlockingRemoveWChunk(null);
		WriteMessage msg = net.getMessageToSend(net
				.getPeerLocation(info.remoteNodeId));
		try {
			msg.writeByte((byte) 5);
			msg.writeLong(info.ticket);
			msg.writeInt(info.submissionId);
			msg.writeInt(info.submissionNode);
			msg.writeInt(info.bucketId);
			msg.writeInt(info.sequence);
			msg.writeLong(info.bucketKey);
			msg.writeInt(info.nrequests);
			msg.writeBoolean(bucket.isSortingBucket());
			msg.writeBoolean(bucket.isSorted());
			msg.writeBoolean(true);
                        msg.writeBoolean(compressing);

                        if (compressing) {
                            int s = tmpBuffer.compressTo(supportBuffer);
                            msg.writeInt(s);
                            msg.writeArray(supportBuffer, 0, s);
                        } else {
                            tmpBuffer.writeTo(new WriteMessageWrapper(msg));
                        }

			boolean isTransfered = buckets.cleanTransfer(info.remoteNodeId,
					info.submissionId, info.bucketId);
			msg.writeBoolean(isTransfered);

			net.finishMessage(msg, info.submissionId);
			bucket.releaseContainer(tmpBuffer);
			if (log.isDebugEnabled()) {
				log.debug("Sent chunk to "
						+ net.getPeerLocation(info.remoteNodeId) + " of size "
						+ tmpBuffer.getNElements() + " be copied at "
						+ info.bucketKey + " req.=" + info.nrequests
						+ " isTransfered=" + isTransfered);
			}
		} catch (IOException e) {
			msg.finish(e);
			throw e;
		}

		tmpBuffer = null;
	}
}
