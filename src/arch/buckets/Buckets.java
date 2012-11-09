package arch.buckets;

import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.StatisticsCollector;
import arch.data.types.Tuple;
import arch.datalayer.TupleIterator;
import arch.net.NetworkLayer;
import arch.storage.Factory;
import arch.storage.container.WritableContainer;

public class Buckets {

	static final Logger log = LoggerFactory.getLogger(Buckets.class);
	private final Map<Long, Bucket> buckets = new HashMap<Long, Bucket>();
	private final Factory<Bucket> bucketsFactory = new Factory<Bucket>(
			Bucket.class);
	Factory<WritableContainer<Tuple>> fb = null;
	CachedFilesMerger merger = null;
	NetworkLayer net = null;
	private final Map<Long, TransferInfo>[] activeTransfers;

	StatisticsCollector stats;

	int internalCounter = Integer.MAX_VALUE;
	int myPartition = 0;

	@SuppressWarnings("unchecked")
	public Buckets(StatisticsCollector stats,
			Factory<WritableContainer<Tuple>> fb, CachedFilesMerger merger,
			NetworkLayer net) {
		this.stats = stats;
		this.fb = fb;
		this.merger = merger;
		this.myPartition = net.getMyPartition();

		this.net = net;
		activeTransfers = new Map[net.getNumberNodes()];
		for (int i = 0; i < activeTransfers.length; ++i) {
			activeTransfers[i] = new HashMap<Long, Buckets.TransferInfo>();
		}
	}

	// Ceriel: added getKey method which ensures that high-order int contains
	// submissionId, even if bucketID < 0.
	public static long getKey(int submissionId, int bucketId) {
		return ((long) submissionId << 32) + (bucketId & 0xFFFFFFFFL);
	}

	public synchronized void clearSubmission(int submissionId) {
		if (buckets.size() > 0) {
			Bucket[] values = buckets.values().toArray(
					new Bucket[buckets.size()]);
			int count = 0;
			long size = 0;
			for (Bucket b : values) {
				if ((int) (b.getKey() >> 32) == submissionId) {
					// Sometimes happens with HashJoin which never gets
					// executed. --Ceriel
					if (b.isFinished()) {
						releaseBucket(b);
						count++;
						size += b.inmemory_size();
					}
				}
			}
			if (count > 0) {
				log.warn("There were still " + count
						+ "  buckets unused of a total size of " + size);
			}
		}
	}

	public synchronized Bucket getOrCreateBucket(int submissionNode,
			int idSubmission, int idBucket, String sortingFunction,
			byte[] sortingParams) {
		long key = getKey(idSubmission, idBucket);
		Bucket bucket = buckets.get(key);

		if (bucket == null) {
			bucket = bucketsFactory.get();
			bucket.init(key, stats, submissionNode, idSubmission,
					sortingFunction, sortingParams, fb, merger);
			buckets.put(key, bucket);
			this.notifyAll();
		}

		return bucket;
	}

	public synchronized Bucket getOrCreateBucket(int submissionNode,
			int idSubmission, String sortingFunction, byte[] sortingParams) {
		return getOrCreateBucket(submissionNode, idSubmission,
				internalCounter++, sortingFunction, sortingParams);
	}

	public synchronized void releaseBucket(Bucket bucket) {
		if (log.isDebugEnabled()) {
			log.debug("releaseBucket: " + bucket.getKey());
		}
		bucket.releaseTuples();
		buckets.remove(bucket.getKey());
		// bucketsFactory.release(bucket);
	}

	public synchronized void clear() {
		// for (Bucket bucket : buckets.values()) {
		// bucketsFactory.release(bucket);
		// }
		buckets.clear();
	}

	public TupleIterator getIterator(int idSubmission, int idBucket/*
																	 * , boolean
																	 * removeDuplicates
																	 */) {
		Bucket bucket = null;
		bucket = getExistingBucket(idSubmission, idBucket);
		BucketIterator itr = new BucketIterator();
		itr.init(bucket, idSubmission, idBucket, this/* , removeDuplicates */);
		return itr;
	}

	public void releaseIterator(BucketIterator itr, boolean forceRelease) {
		// Ceriel: changed order: read bucket from iterator and release it
		// before releasing the iterator (which may corrupt the bucket field).
		if (log.isDebugEnabled()) {
			log.debug("Releasing " + itr);
		}
		// If itr.isUsed is false, this iterator was just there to wait for
		// availability.
		// So, don't kill the bucket!
		if (forceRelease
				|| (itr.isUsed && itr.bucket.isFinished() && itr.bucket
						.isEmpty())) {
			// releaseBucket(itr.bucket);
			itr.bucket = null;
		}
	}

	public synchronized Bucket getBucket(long bucketKey) {
		return buckets.get(bucketKey);
	}

	public synchronized Bucket getExistingBucket(long bucketKey) {
		Bucket bucket = buckets.get(bucketKey);

		while (bucket == null) {
			// wait until somebody else will create it
			try {
				if (log.isDebugEnabled()) {
					log.debug("Waiting for bucket " + bucketKey + " to appear");
				}
				while (bucket == null) {
					this.wait();
					bucket = buckets.get(bucketKey);
				}
				if (log.isDebugEnabled()) {
					log.debug("Got bucket " + bucketKey);
				}
			} catch (Exception e) {
				log.error("Error", e);
			}
		}

		return bucket;
	}

	public synchronized Bucket getExistingBucket(int submissionId, int bucketId) {
		return getExistingBucket(getKey(submissionId, bucketId));
	}

	// public synchronized Bucket findExistingBucket(int submissionId, int
	// bucketId) {
	// return buckets.get(getKey(submissionId, bucketId));
	// }

	private static class TransferInfo {
		int count = 1;
		Bucket bucket;
		boolean alerted = false;
	}

	public Bucket startTransfer(int submissionNode, int submission, int node,
			int bucketID, String sortingFunction, byte[] sortingParams) {

		if (node == myPartition || net.getNumberNodes() == 1) {
			// return directly the node
			return getOrCreateBucket(submissionNode, submission, bucketID,
					sortingFunction, sortingParams);
		}

		Map<Long, TransferInfo> map = activeTransfers[node];

		long key = getKey(submission, bucketID);
		TransferInfo info = null;

		synchronized (map) {
			info = map.get(key);
			if (info == null) {
				// There is any transfer active. Create one
				info = new TransferInfo();
				info.bucket = getOrCreateBucket(submissionNode, submission,
						sortingFunction, sortingParams);
				map.put(key, info);
			} else {
				info.count++;
			}
		}
		return info.bucket;
	}

	public void finishTransfer(int submissionNode, int submission, int node,
			int bucketID, long chainId, long parentChainId, int nchildren,
			int replicatedFactor, boolean responsible, String sortingFunction,
			byte[] sortingParams, boolean decreaseCounter) throws IOException {

		if (node == myPartition || net.getNumberNodes() == 1) {
			Bucket bucket = getOrCreateBucket(submissionNode, submission,
					bucketID, sortingFunction, sortingParams);

			bucket.updateCounters(chainId, parentChainId, nchildren,
					replicatedFactor, responsible);
			bucket.updateCounters(0, true);
			return;
		}

		Map<Long, TransferInfo> map = activeTransfers[node];

		long key = getKey(submission, bucketID);
		TransferInfo info = null;

		// Alert the node that there is an active transfer
		WriteMessage message = net.getMessageToSend(net.getPeerLocation(node),
				NetworkLayer.nameMgmtReceiverPort);
		message.writeByte((byte) 1); // Mark to indicate there are tuples
		message.writeInt(submissionNode);
		message.writeInt(submission);
		message.writeInt(bucketID); // Remote bucket
		message.writeLong(chainId);
		message.writeLong(parentChainId);
		message.writeInt(nchildren);
		message.writeInt(replicatedFactor);
		message.writeBoolean(responsible);
		if (sortingFunction == null || sortingFunction.equals("")) {
			message.writeBoolean(false);
		} else {
			message.writeBoolean(true);
			message.writeString(sortingFunction);
			if (sortingParams != null && sortingParams.length > 0) {
				message.writeInt(sortingParams.length);
				message.writeArray(sortingParams);
			} else {
				message.writeInt(0);
			}
		}

		synchronized (map) {
			info = map.get(key);

			if (info == null || info.alerted) {
				// There was no triple in the bucket
				message.writeLong(-1); // Local buffer ID
			} else {
				// There was something in the bucket. The remote node has
				// already been alerted
				info.alerted = true;
				message.writeLong(info.bucket.getKey()); // Local buffer ID
			}

			if (info != null && decreaseCounter) {
				info.count--;
			}
		}

		net.finishMessage(message, submission);
	}

	public boolean cleanTransfer(int nodeId, int submissionId, int bucketId) {

		Map<Long, TransferInfo> map = activeTransfers[nodeId];

		long key = getKey(submissionId, bucketId);
		TransferInfo info = null;

		synchronized (map) {
			info = map.get(key);

			if (info.count == 0 && info.bucket.isEmpty()) {
				// Clean and return true
				map.remove(key);
				releaseBucket(info.bucket);
				return true;
			} else {
				return false;
			}
		}
	}

	public boolean isActiveTransfer(int submissionId, int nodeId, int bucketId) {
		Map<Long, TransferInfo> map = activeTransfers[nodeId];

		long key = getKey(submissionId, bucketId);
		TransferInfo info = null;
		synchronized (map) {
			info = map.get(key);
			return info != null && info.count > 0;
		}

	}
}
