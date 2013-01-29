package nl.vu.cs.ajira.net;

import ibis.ipl.WriteMessage;
import ibis.util.ThreadPool;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.buckets.Buckets;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TupleSender {

	static final Logger log = LoggerFactory.getLogger(TupleSender.class);

	final NetworkLayer net;
	final Buckets buckets;
	final Factory<TupleInfo> tuFactory = new Factory<TupleInfo>(TupleInfo.class);

	private final List<TupleInfo> checkList = new LinkedList<TupleInfo>();
	private final List<TupleInfo> sendList = new LinkedList<TupleInfo>();
	private int checkerTime = 1;

	Factory<WritableContainer<Tuple>> bufferFactory;

	public TupleSender(Context context,
			Factory<WritableContainer<Tuple>> bufferFactory) {
		this.net = context.getNetworkLayer();
		this.buckets = context.getBuckets();
		this.bufferFactory = bufferFactory;
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
			}, "TupleSender");
		}
	}

	// This method should be called from a thread that may block.
	public void handleNewRequest(long localBufferKey, int remoteNodeId,
			int idSubmission, int idBucket, long ticket, int sequence,
			int nrequest) {
		final TupleInfo tu = tuFactory.get();

		tu.bucketKey = localBufferKey;
		tu.remoteNodeId = remoteNodeId;
		tu.submissionId = idSubmission;
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
					Bucket bucket = buckets.getExistingBucket(info.bucketKey,
							false);
					if (bucket != null) {
						boolean enoughData = bucket.availableToTransmit()
								|| !buckets.isActiveTransfer(info.submissionId,
										info.remoteNodeId, info.bucketId);
						if (enoughData) {
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

	private void sendTuples() {
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
				sendTuple(info);
			} catch (IOException e) {
				log.warn("Got IOException in tuple sender", e);
			}
		}
	}

	private void sendTuple(TupleInfo info) throws IOException {
		// long time = System.currentTimeMillis();
		WritableContainer<Tuple> tmpBuffer = bufferFactory.get();
		tmpBuffer.clear();
		Bucket bucket = buckets.getExistingBucket(info.bucketKey, false);
		// long timeMsg = System.currentTimeMillis();
		bucket.removeChunk(tmpBuffer);
		// long timeRetrieve = System.currentTimeMillis() - timeMsg;
		WriteMessage msg = net.getMessageToSend(net
				.getPeerLocation(info.remoteNodeId));
		msg.writeByte((byte) 5);
		msg.writeLong(info.ticket);
		msg.writeInt(info.submissionId);
		msg.writeInt(info.bucketId);
		msg.writeInt(info.sequence);
		msg.writeLong(info.bucketKey);
		msg.writeInt(info.nrequests);
		if (bucket.shouldSort()) {
			msg.writeBoolean(true);
		} else {
			msg.writeBoolean(false);
		}
		msg.writeBoolean(true);
		tmpBuffer.writeTo(new WriteMessageWrapper(msg));

		boolean isTransfered = buckets.cleanTransfer(info.remoteNodeId,
				info.submissionId, info.bucketId);
		msg.writeBoolean(isTransfered);

		net.finishMessage(msg, info.submissionId);

		if (log.isDebugEnabled()) {
			log.debug("Sent chunk to " + net.getPeerLocation(info.remoteNodeId)
					+ " of size " + tmpBuffer.getNElements() + " ("
					+ tmpBuffer.bytesToStore() + ") " + " be copied at "
					+ info.bucketKey + " req.=" + info.nrequests
					+ " isTransfered=" + isTransfered
			// + " Total t.: "
			// + (System.currentTimeMillis() - time)
			// + " time w. msg: "
			// + (System.currentTimeMillis() - timeMsg)
			// + " time r. buffer: " + timeRetrieve
			);
		}

		// bufferFactory.release(tmpBuffer);
		tuFactory.release(info);
	}
}
