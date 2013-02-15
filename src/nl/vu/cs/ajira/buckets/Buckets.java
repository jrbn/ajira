package nl.vu.cs.ajira.buckets;

import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.mgmt.StatisticsCollector;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains the node's primary primitives for managing the buckets
 * layer: - create a bucket, - release a bucket, - get a bucket, - get the
 * iterator, - broadcast a signal if there are tuples to transfer (start,
 * finish, clean transfer)
 * 
 * Basically is a wrapper class that is used to control all the primary
 * functions regarding the bucket's data structure.
 */
public class Buckets {
	static final Logger log = LoggerFactory.getLogger(Buckets.class);
	private final Map<Long, Bucket> buckets = new HashMap<Long, Bucket>();

	// Buffer factories
	@SuppressWarnings("unchecked")
	Class<WritableContainer<TupleSerializer>> clazz = (Class<WritableContainer<TupleSerializer>>) (Class<?>) WritableContainer.class;
	Factory<WritableContainer<TupleSerializer>> fb_not_sorted = new Factory<WritableContainer<TupleSerializer>>(
			clazz, Consts.TUPLES_CONTAINER_BUFFER_SIZE);;
	Factory<WritableContainer<TupleSerializer>> fb_sorted = new Factory<WritableContainer<TupleSerializer>>(
			clazz, true, Consts.TUPLES_CONTAINER_BUFFER_SIZE);;

	CachedFilesMerger merger = null;
	NetworkLayer net = null;
	private final Map<Long, TransferInfo>[] activeTransfers;

	StatisticsCollector stats;

	int myPartition = 0;

	/**
	 * Custom constructor.
	 * 
	 * @param stats
	 *            Collection in which we add/aggregate counters (statistics)
	 * @param merger
	 *            Merger that is being used for merging the cached files created
	 *            by the buckets of this node
	 * @param net
	 *            Network layer
	 */
	@SuppressWarnings("unchecked")
	public Buckets(StatisticsCollector stats, CachedFilesMerger merger,
			NetworkLayer net) {
		this.stats = stats;
		this.merger = merger;
		this.myPartition = net.getMyPartition();
		this.net = net;
		activeTransfers = new Map[net.getNumberNodes()];
		for (int i = 0; i < activeTransfers.length; ++i) {
			activeTransfers[i] = new HashMap<Long, Buckets.TransferInfo>();
		}
	}

	/**
	 * Method that generates a key for uniquely identifying a buffer -- is
	 * generated using the bucket's submission identifier and also its own
	 * identifier. Ensures that high-order int contains submissionId, even if
	 * bucketID < 0.
	 * 
	 * @param submissionId
	 *            Submission id
	 * @param bucketId
	 *            Bucket id
	 * @return
	 */
	private static long getKey(int submissionId, int bucketId) {
		return ((long) submissionId << 32) + (bucketId & 0xFFFFFFFFL);
	}

	/**
	 * Removes all the buckets (the local and remote ones) owned by this node.
	 * For each finished registered bucket we call the releaseBucket() method.
	 * 
	 * @see #releaseBucket
	 * 
	 * @param submissionId
	 *            Submission id -- used to identify the bucket
	 */
	public synchronized void removeBucketsOfSubmission(int submissionId) {
		if (buckets.size() > 0) {
			Bucket[] values = buckets.values().toArray(
					new Bucket[buckets.size()]);
			int count = 0;
			long size = 0;
			for (Bucket b : values) {
				if ((int) (b.getKey() >> 32) == submissionId) {
					// Sometimes happens with HashJoin which never gets
					// executed.
					// if (b.isFinished()) {
						releaseBucket(b);
						count++;
						size += b.inmemory_size();
					// }
				}
			}
			if (count > 0) {
				log.warn("There were still " + count
						+ "  buckets unused of a total size of " + size);
			}
		}
	}

	/**
	 * This method is used to get (if it does already exists) or create (if it
	 * does not) a bucket. Each bucket has associated a unique key which is used
	 * to check if it has been already created -- if not, then we generate a new
	 * one using the factory.
	 * 
	 * @param submissionNode
	 *            Submission node - remote node responsible with this
	 *            remote-bucket (we create a remote-bucket on this node's side)
	 * @param idSubmission
	 *            Submission id
	 * @param idBucket
	 *            Bucket id
	 * @param sort
	 *            Activate sort or not on the bucket
	 * @param sortingFields
	 *            What fields to sort on
	 * @param signature
	 *            The signature used for defining the sort order between the
	 *            fields
	 * @return the bucket
	 */
	public synchronized Bucket getOrCreateBucket(int submissionNode,
			int idSubmission, int idBucket, boolean sort, byte[] sortingFields,
			byte[] signature) {
		long key = getKey(idSubmission, idBucket);
		Bucket bucket = buckets.get(key);

		if (bucket == null) {
			bucket = new Bucket();
			if (sort) {
				bucket.init(key, stats, submissionNode, idSubmission, sort,
						sortingFields, fb_sorted, merger, signature);
			} else {
				bucket.init(key, stats, submissionNode, idSubmission, sort,
						sortingFields, fb_not_sorted, merger, signature);
			}
			buckets.put(key, bucket);
			this.notifyAll();
		}

		return bucket;
	}

	/**
	 * Releases a bucket: removes the bucket from the buckets' collection and
	 * releases the internal buffer also.
	 * 
	 * @param bucket
	 *            Bucket to be released
	 */
	public synchronized void releaseBucket(Bucket bucket) {
		if (log.isDebugEnabled()) {
			log.debug("releaseBucket: " + bucket.getKey());
		}

		bucket.releaseTuples();
		buckets.remove(bucket.getKey());
	}

	/**
	 * Returns the iterator of a bucket. The bucket, which iterator is
	 * retrieved, is identified by the submission id and its id.
	 * 
	 * @param c
	 *            The current ActionContext
	 * @param idBucket
	 *            Bucket id
	 * @return Bucket's iterator
	 */
	public TupleIterator getIterator(ActionContext c, int idBucket) {
		Bucket bucket = null;
		bucket = getExistingBucket(c.getSubmissionId(), idBucket);
		BucketIterator itr = new BucketIterator();
		itr.init(c, bucket, bucket.getSignature(), this);
		return itr;
	}

	/**
	 * Releases a bucket-iterator given by parameter -- only if the iterator is
	 * not used and the buckets is flagged with finished and emptied of its
	 * content.
	 * 
	 * @param itr
	 *            Iterator to be released (passing by value)
	 */
	public void releaseIterator(BucketIterator itr) {
		// Changed order: read bucket from iterator and release it
		// before releasing the iterator (which may corrupt the bucket field).
		if (log.isDebugEnabled()) {
			log.debug("Releasing " + itr);
		}

		// If itr.isUsed is false, this iterator was just there to wait for
		// availability.
		// So, don't kill the bucket!
		if (itr.isUsed && itr.bucket.isFinished() && itr.bucket.isEmpty()) {
			itr.bucket = null;
		}
	}

	/**
	 * Method that gets an existent bucket given it's key. If it does not exist
	 * 'null' and if we do not want to wait until is created 'null' is returned,
	 * otherwise we enter into wait() until someone created the bucket.
	 * 
	 * @param bucketKey
	 *            Bucket's key - to identify the bucket
	 * @param wait
	 *            True/false if we want to wait or not for the bucket's
	 *            'external' creation
	 * @return The bucket that was requested
	 */
	public synchronized Bucket getExistingBucket(long bucketKey, boolean wait) {
		Bucket bucket = buckets.get(bucketKey);

		while (wait && bucket == null) {
			// Wait until somebody else will create it
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

	/**
	 * Method that gets an existent bucket given the submission id and its
	 * bucket id. With those two identifiers we compose the key and we use the
	 * other getExistentBuckets(key, wait) to retrieve the bucket's reference.
	 * 
	 * @param submissionId
	 *            Submission id
	 * @param bucketId
	 *            Bucket id
	 * @return The bucket that was requested
	 */
	public synchronized Bucket getExistingBucket(int submissionId, int bucketId) {
		return getExistingBucket(getKey(submissionId, bucketId), true);
	}

	/**
	 * This class contains transfer information about the remote-bucket: count:
	 * how many transfers have been started bucket: the remote-bucket being
	 * involved in the transfer alerted: if the node responsible for the
	 * remote-bucket was alerted or not
	 */
	private static class TransferInfo {
		int count = 1;
		Bucket bucket;
		boolean alerted = false;
	}

	/**
	 * This method setups the bucket that is going to be used for the tuples'
	 * transfer. For each one of the remote-buckets it also creates transfer
	 * info records (stored in a hash map using the bucket's key)
	 * 
	 * @see TransferInfo
	 * 
	 * @param submissionNode
	 *            The remote node's id
	 * @param submission
	 *            Submission id
	 * @param node
	 *            Node's id (is a simple number from 0 to N) (--local node)
	 * @param bucketID
	 *            Bucket id
	 * @param sort
	 *            Activate sort or not on the bucket
	 * @param sortingFields
	 *            What fields to sort on
	 * @param signature
	 *            The signature used for defining the sort order between the
	 *            fields
	 * @param context
	 *            Context of the action
	 * @return The bucket prepared for the transfer
	 */
	public Bucket startTransfer(int submissionNode, int submission, int node,
			int bucketID, boolean sort, byte[] sortingFields, byte[] signature,
			ActionContext context) {

		if (node == myPartition || net.getNumberNodes() == 1) {
			return getOrCreateBucket(submissionNode, submission, bucketID,
					sort, sortingFields, signature);
		}

		Map<Long, TransferInfo> map = activeTransfers[node];
		long key = getKey(submission, bucketID);
		TransferInfo info = null;

		synchronized (map) {
			info = map.get(key);

			if (info == null) {
				// There is no transfer active. Create one.
				info = new TransferInfo();
				info.bucket = getOrCreateBucket(submissionNode, submission,
						context.getNewBucketID(), sort, sortingFields,
						signature);
				map.put(key, info);
			} else {
				info.count++;
			}
		}

		return info.bucket;
	}

	/**
	 * This method is used, at the end of a partition reading, to update the
	 * counters on the local-bucket and to broadcast alerts/signals about the
	 * active transfer of the remote-buckets. In the alert/signal being sent we
	 * also indicate that there are tuples available for transfer. Before
	 * broadcasting the message, the information about the transfer is updated.
	 * 
	 * @param submissionNode
	 *            Node responsible for the remote-bucket
	 * @param submission
	 *            Submission id
	 * @param node
	 *            Node's id
	 * @param bucketID
	 *            Bucket id
	 * @param chainId
	 *            Chain id
	 * @param parentChainId
	 *            Chain's parent id
	 * @param nchildren
	 *            Number of chain's children
	 * @param responsible
	 *            True/false if the current chain is a root-chain or not
	 * @param sort
	 *            Activate sort or not on the bucket
	 * @param sortingFields
	 *            What fields to sort on
	 * @param signature
	 *            The signature used for defining the sort order between the
	 *            fields
	 * @param decreaseCounter
	 *            True/false if we need to decrease the transfer info counter or
	 *            not
	 * @throws IOException
	 */
	public void finishTransfer(int submissionNode, int submission, int node,
			int bucketID, long chainId, long parentChainId, int nchildren,
			boolean responsible, boolean sort, byte[] sortingFields,
			byte[] signature, boolean decreaseCounter) throws IOException {

		if (node == myPartition || net.getNumberNodes() == 1) {
			Bucket bucket = getOrCreateBucket(submissionNode, submission,
					bucketID, sort, sortingFields, signature);

			bucket.updateCounters(chainId, parentChainId, nchildren,
					responsible);
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
		message.writeBoolean(responsible);
		if (!sort) {
			message.writeBoolean(false);
		} else {
			message.writeBoolean(true);
			if (sortingFields != null && sortingFields.length > 0) {
				message.writeInt(sortingFields.length);
				message.writeArray(sortingFields);
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

		message.writeByte((byte) signature.length);
		message.writeArray(signature);

		net.finishMessage(message, submission);
	}

	/**
	 * This method is used to clean all the transfer information recorded for a
	 * remote-bucket.
	 * 
	 * @param nodeId
	 *            Node's id (--local node)
	 * @param submissionId
	 *            Submission id
	 * @param bucketId
	 *            Bucket id
	 * @return True/false whether the clean was successful or not
	 */
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

	/**
	 * Checks if the bucket's transfer is still active -- if there are anymore
	 * buckets in the middle of a transfer.
	 * 
	 * @param submissionId
	 *            Submission id
	 * @param nodeId
	 *            Node's id (--local node)
	 * @param bucketId
	 *            Bucket id
	 * @return True/false whether the transfer is still active or not
	 */
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
