package nl.vu.cs.ajira.buckets;

import ibis.util.ThreadPool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.chains.ChainNotifier;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.bytearray.FDataInput;
import nl.vu.cs.ajira.data.types.bytearray.FDataOutput;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.mgmt.StatisticsCollector;
import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.RawComparator;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the data structure used to store tuples in the main
 * memory and to manage caching/storing on the disk when the size exceeds the
 * maximum limit. Basically, it contains a buffer and a map over the files that
 * were used to spill tuples on the disk.
 */
public class Bucket {

	public static final int N_WBUFFS = 2;

	private static class WriteBuffer {
		WritableContainer<WritableTuple> buffer;
		boolean removeChunkReturned;
		WriteBuffer next;
	}

	Context context;

	private boolean startedReading;
	private WriteBuffer freeList;
	private WriteBuffer availableList;
	private final Object freeListLock = new Object();
	private final Object availableListLock = new Object();

	static final Logger log = LoggerFactory.getLogger(Bucket.class);

	// Used for unsorted streams.
	private final List<File> cacheFiles = new ArrayList<File>();
	private final Map<Long, Integer> children = new HashMap<Long, Integer>();

	// Used for sorting
	private boolean sort;
	private byte[] sortingFields = null;
	private final TupleComparator comparator = new TupleComparator();
	private byte[] signature;
	private WritableTuple serializer;

	private long elementsInCache = 0;
	private SortedBucketCache sortedBucketCache;

	Factory<WritableContainer<WritableTuple>> fb = null;
	boolean gettingData;
	private int highestSequence;

	private boolean isFinished;

	private TupleIterator iter;
	private long key;

	private Map<Long, List<Integer>> additionalChildrenCounts = null;

	private int nBucketReceived = 0;
	private int nChainsReceived = 0;

	private ChainNotifier notifier;

	int numCachers;
	int numWaitingForCachers;

	private boolean receivedMainChain;

	private byte[] sequencesReceived = new byte[Consts.MAX_SEGMENTS_RECEIVED];

	private StatisticsCollector stats;
	int submissionId;
	int submissionNode;
	// Internal tuples - assigned tuples read from local files
	private WritableContainer<WritableTuple> inBuffer = null;
	// External tuples - assigned tuples pulled from remote files
	private WritableContainer<WritableTuple> exBuffer = null;
	private boolean isInBufferSorted = true;
	private boolean isExBufferSorted = true;

	private boolean isFillingThreadStarted;
	private int waitersForAdditions;

	private long totalNumberOfElements = 0;

	boolean sortingBucket;
	private boolean done;

	private boolean streaming;

	/**
	 * This method is used to add a tuple to the in-memory buffer. If the
	 * element cannot be added on the first attempt, the buffer has to be
	 * spilled on disk and then retry to add it again. If this action also fails
	 * than an exception is raised.
	 * 
	 * @param tuple
	 *            The tuple that has to be inserted in the bucket
	 * @return True if the element could be added into the buffer, or false
	 *         (actually never -- an Exception is thrown instead)
	 * 
	 * @throws Exception
	 */
	public synchronized boolean add(Tuple tuple) throws Exception {
		if (inBuffer == null) {
			inBuffer = getContainer();
		}
		totalNumberOfElements++;

		serializer.setTuple(tuple);
		boolean response = inBuffer.add(serializer);

		if (response) {
			isInBufferSorted = inBuffer.getNElements() < 2;
		} else {
			cacheBuffer(inBuffer, isInBufferSorted);
			inBuffer = getContainer();
			response = inBuffer.add(serializer);
			isInBufferSorted = true;

			if (!response) {
				throw new Exception(
						"The buffer is too small! Must increase the buffer size.");
			}
		}

		if (!sort) {
			if (!isFillingThreadStarted) {
				prepareRemoveWChunk();
			}

			if (waitersForAdditions > 0
					&& (streaming || inBuffer.getRawSize() > Consts.MIN_SIZE_TO_SEND)) {
				notifyAll();
			}

			if (notifier != null) {
				notifier.markReady(iter);
				notifier = null;
				iter = null;
			}
		}
		return response;
	}

	/**
	 * This method is used to add an external buffer (a new tuples container)
	 * into the local one. Instead of doing one add per each new tuple, we copy
	 * the entire content of the new container into the bucket. Mainly, this
	 * method is used to add a whole chunk transferred from a remote bucket.
	 * This method may reuse the newTuplesContainer, so after calling this
	 * method, you can no longer use the container.
	 * 
	 * @param newTuplesContainer
	 *            New buffer that has to be inserted
	 * @param isSorted
	 *            True/false if whether the tuples from inside the container are
	 *            sorted or not
	 */
	public synchronized void addAll(
			WritableContainer<WritableTuple> newTuplesContainer,
			boolean isSorted) {

		totalNumberOfElements += newTuplesContainer.getNElements();

		long time = System.currentTimeMillis();

		if (exBuffer == null || exBuffer.getNElements() == 0) {
			if (log.isDebugEnabled()) {
				log.debug("addAll: adding a sorted = " + isSorted
						+ " buffer with " + newTuplesContainer.getNElements()
						+ " elements " + "to empty exBuffer");
			}
			releaseExBuffer();
			exBuffer = newTuplesContainer;
			isExBufferSorted = isSorted;
		} else {
			// LOG-DEBUG
			if (log.isDebugEnabled()) {
				log.debug("addAll: adding a sorted = " + isSorted
						+ " buffer with " + newTuplesContainer.getNElements()
						+ " elements " + "TO exBuffer, sorted = "
						+ isExBufferSorted + " with " + exBuffer.getNElements()
						+ " elements");
			}

			if (newTuplesContainer.getNElements() > exBuffer.getNElements()) {
				if (newTuplesContainer.addAll(exBuffer)) {
					releaseExBuffer();
					exBuffer = newTuplesContainer;
					isExBufferSorted = false;
				} else {
					cacheBuffer(newTuplesContainer, isSorted);
				}
			} else {
				if (exBuffer.addAll(newTuplesContainer)) {
					releaseContainer(newTuplesContainer);
					isExBufferSorted = false;
				} else {
					cacheBuffer(exBuffer, isExBufferSorted);
					exBuffer = newTuplesContainer;
					isExBufferSorted = isSorted;
				}
			}
		}

		if (!sort) {
			if (!isFillingThreadStarted) {
				prepareRemoveWChunk();
			}

			if (waitersForAdditions > 0) {
				notifyAll();
			}

			if (notifier != null) {
				notifier.markReady(iter);
				notifier = null;
				iter = null;
			}
		}

		stats.addCounter(submissionNode, submissionId,
				"Bucket:addAll: overall time (ms)", System.currentTimeMillis()
						- time);
	}

	/**
	 * Method that checks if there are tuples available to be transferred.
	 * 
	 * @return true/false whether the bucket has enough bytes to transfer or
	 *         there are some cached files and we prefer to empty the buffer
	 */
	public boolean availableToTransmit() {
		synchronized (availableListLock) {
			return availableList != null || done;
		}
	}

	/**
	 * Method that is used to cache the content of a buffer into files on disk
	 * whenever its size exceeds a maximum limit. The idea is to spill on disk
	 * the entire in-memory buffer when its size exceeds a limit (when no tuple
	 * can be inserted anymore - the response will be false so this method is
	 * called for caching the buffer before retrying to add the tuple). For
	 * doing that we first sort the buffer (if necessary - depends on the
	 * 'sorted' param) and then we open a temporary file (input stream) for
	 * writing it's content down. All the information/details (filename, size,
	 * etc) about the caching operation are kept as metadata into a hash
	 * data-structure. @see FileMetaData
	 * 
	 * If the number of temporary files are greater than a fixed limit, a
	 * background thread that merges those files is started in order to get
	 * their number back to normal. @see CachedFilesMerger
	 * 
	 * @param buffer
	 *            Buffer (tuples container) to be cached
	 * @param sorted
	 *            True/false if the tuples inside the buffer are sorted or not
	 */
	private void cacheBuffer(final WritableContainer<WritableTuple> buffer,
			final boolean sorted) {
		if (buffer.getNElements() == 0) {
			releaseContainer(buffer);
			return;
		}

		synchronized (this) {
			elementsInCache += buffer.getNElements();
			numCachers++;
		}

		ThreadPool.createNew(new Runnable() {
			@Override
			public void run() {
				try {
					if (sort && !sorted) {
						TupleComparator c = new TupleComparator();
						comparator.copyTo(c);
						buffer.sort(c, fb);
					}

					long time = System.currentTimeMillis();

					if (!sort) {
						File cacheFile = File.createTempFile("cache", "tmp");
						cacheFile.deleteOnExit();

						OutputStream fout = new SnappyOutputStream(
								new BufferedOutputStream(new FileOutputStream(
										cacheFile)));

						FDataOutput cacheOutputStream = new FDataOutput(fout);
						buffer.writeTo(cacheOutputStream);
						cacheOutputStream.close();
						synchronized (Bucket.this) {
							cacheFiles.add(cacheFile);
						}
					} else {
						sortedBucketCache.cacheBuffer(buffer);
					}

					stats.addCounter(submissionNode, submissionId,
							"Time spent writing to cache (ms)",
							System.currentTimeMillis() - time);
					releaseContainer(buffer);
				} catch (Throwable e) {
					if (log.isDebugEnabled()) {
						log.debug("Got exception while writing cache!", e);
					}
					context.killSubmission(submissionNode, submissionId, e);
				}
				synchronized (Bucket.this) {
					numCachers--;
					if (numCachers == 0 && numWaitingForCachers > 0) {
						Bucket.this.notifyAll();
					}
				}
			}
		}, "Sort-and-cache for bucket " + key);

	}

	/**
	 * This method is used to internally check if all the send-receive
	 * operations performed over the bucket are finished.
	 */
	private void checkFinished() {

		if (log.isDebugEnabled()) {
			log.debug("checkFinished " + this.key + ": nChainsReceived = "
					+ nChainsReceived + ", nBucketReceived = "
					+ nBucketReceived + ", highestSequence = "
					+ highestSequence + ", children = " + children.toString()
					+ ", receivedMainChain = " + receivedMainChain);
		}
		if (nChainsReceived == nBucketReceived && highestSequence != -1
				&& children.size() == 0 && receivedMainChain) {
			for (int i = 0; i < highestSequence + 1; ++i)
				if (sequencesReceived[i] != 0) {
					if (log.isDebugEnabled()) {
						log.debug("sequencesReceived[" + i + "] = "
								+ sequencesReceived[i]);
					}
					return;
				}
			if (log.isDebugEnabled()) {
				log.debug("Calling setFinished on bucket " + this.key);
			}
			setFinished();
		}
	}

	public long getKey() {
		return key;
	}

	public byte[] getSortingFields() {
		return sortingFields;
	}

	/**
	 * Initialization method for the bucket's private/public variables and
	 * objects.
	 * 
	 * @param key
	 *            Bucket key
	 * @param stats
	 *            Statistics collection -- where to aggregate the statistics
	 * @param submissionNode
	 *            Node responsible with the remote-bucket
	 * @param submissionId
	 *            Submission id
	 * @param sort
	 *            Activate sort or not on the bucket
	 * @param sortingFields
	 *            What fields to sort on
	 * @param fb
	 *            Factory used to generate buffers -- a pool of unused buffers
	 * @param merger
	 *            Cache file merger
	 * @param signature
	 *            The signature used for defining the sort order between the
	 *            fields
	 */
	@SuppressWarnings("unchecked")
	void init(long key, Context context, StatisticsCollector stats,
			int submissionNode, int submissionId, boolean sort,
			boolean sortRemote, boolean streaming, byte[] sortingFields,
			Factory<WritableContainer<WritableTuple>> fb,
			CachedFilesMerger merger, byte[] signature) {
		this.sortingBucket = sort || sortRemote;
		this.key = key;
		this.fb = fb;
		this.inBuffer = null;
		this.context = context;
		this.streaming = streaming;

		isFinished = false;
		receivedMainChain = false;
		nChainsReceived = 0;
		nBucketReceived = 0;
		highestSequence = -1;
		gettingData = false;
		waitersForAdditions = 0;

		notifier = null;
		iter = null;

		elementsInCache = 0;

		cacheFiles.clear();
		children.clear();

		done = false;

		additionalChildrenCounts = null;

		this.stats = stats;
		this.submissionNode = submissionNode;
		this.submissionId = submissionId;

		isFillingThreadStarted = false;
		isInBufferSorted = true;
		this.sort = sort;
		this.signature = signature;
		if (sort) {
			this.sortingFields = sortingFields;
			// Retrieve suitable comparators for the fields to sort
			RawComparator<? extends SimpleData>[] array = null;

			if (sortingFields != null) {
				array = new RawComparator[sortingFields.length];
				for (int i = 0; i < sortingFields.length; ++i) {
					array[i] = RawComparator
							.getComparator(signature[sortingFields[i]]);
				}
			} else {
				array = new RawComparator[signature.length];
				for (int i = 0; i < signature.length; ++i) {
					array[i] = RawComparator.getComparator(signature[i]);
				}
			}

			this.comparator.init(array);
			this.serializer = new WritableTuple(sortingFields, signature.length);
			sortedBucketCache = new SortedBucketCache(comparator, this, merger);
		} else if (sortRemote) {
			this.serializer = new WritableTuple(sortingFields, signature.length);
		} else {
			this.serializer = new WritableTuple();
		}
		// for (int i = 0; i < N_WBUFFS; i++) {
		WriteBuffer w = new WriteBuffer();
		w.next = freeList;
		freeList = w;
		// }
	}

	public synchronized long inmemory_size() {
		if (inBuffer == null) {
			if (exBuffer != null) {
				return exBuffer.getTotalCapacity();
			}

			return 0;
		} else {
			if (exBuffer == null) {
				return inBuffer.getTotalCapacity();
			}

			return (inBuffer.getTotalCapacity() + exBuffer.getTotalCapacity());
		}
	}

	public synchronized boolean isEmpty() {
		if (log.isDebugEnabled()) {
			log.debug("isEmpty() on bucket " + key
					+ ": totalNumberOfElements = " + totalNumberOfElements);
		}
		return totalNumberOfElements == 0;

	}

	synchronized boolean isFinished() {
		return isFinished;
	}

	/**
	 * Registers the bucket as being finished and notifies the iterator (it
	 * marks as ready).
	 * 
	 * @param notifier
	 * @param iter
	 */
	synchronized void registerFinishedNotifier(ChainNotifier notifier,
			TupleIterator iter) {

		if (isFinished()) {
			notifier.markReady(iter);
			return;
		}

		this.notifier = notifier;
		this.iter = iter;
	}

	/**
	 * Release the buffer -- GC.
	 */
	synchronized void releaseBuffers() {
		waitForCachersToFinish();
		isFinished = true;
		notifyAll();
		releaseInBuffer();
		releaseExBuffer();
		// Also remove files.
		if (sortedBucketCache != null) {
			sortedBucketCache.finished();
		}
	}

	// TODO: should be part of WritableContainer I think
	private synchronized void releaseExBuffer() {
		if (exBuffer != null) {
			releaseContainer(exBuffer);
			exBuffer = null;
		}
	}

	// TODO: should be part of WritableContainer I think
	private synchronized void releaseInBuffer() {
		if (inBuffer != null) {
			releaseContainer(inBuffer);
			inBuffer = null;
		}
	}

	private synchronized WritableContainer<WritableTuple> removeChunkUnsorted(
			boolean[] done) throws Exception {

		long totTime = System.currentTimeMillis();
		WritableContainer<WritableTuple> retval = null;

		for (;;) {
			// If some threads still have to finish writing
			waitForCachersToFinish();
			if (elementsInCache > 0) {
				long time = System.currentTimeMillis();
				File fi = null;

				if (cacheFiles.size() > 0) {
					fi = cacheFiles.remove(0);
				}
				if (retval == null) {
					retval = getContainer();
				}
				if (fi != null) {
					FDataInput di = new FDataInput(new SnappyInputStream(
							new BufferedInputStream(new FileInputStream(fi))));
					retval.readFrom(di); // Read the oldest file
					stats.addCounter(submissionNode, submissionId,
							"Bucket:removeChunk: time reading from cache (ms)",
							System.currentTimeMillis() - time);
					stats.addCounter(submissionNode, submissionId,
							"Bucket:removeChunk: Bytes read from cache",
							retval.getRawSize());
					elementsInCache -= retval.getNElements();
					di.close();
					fi.delete();
				}
			}

			// Let's see if we can also get the data either from the
			// inBuffer, or exBuffer, but then we must be sure they are not
			// being cached.
			if (inBuffer != null && inBuffer.getNElements() != 0) {
				if (retval == null) {
					retval = inBuffer;
					inBuffer = null;
				} else {
					if (retval.addAll(inBuffer)) {
						releaseInBuffer();
					}
				}
			}
			if (exBuffer != null && exBuffer.getNElements() != 0) {
				if (retval == null) {
					retval = exBuffer;
					exBuffer = null;
				} else {
					if (retval.addAll(exBuffer)) {
						releaseExBuffer();
					}
				}
			}

			// There was nothing available. Now we do not have any choice than
			// wait.

			if (retval == null || retval.getNElements() == 0) {
				if (!isFinished) {
					waitersForAdditions++;
					this.wait(50);
					waitersForAdditions--;
					// It became finished while I was executing the previous
					// code. Let's try again.
					continue;
				}
			}

			long tm = System.currentTimeMillis() - totTime;
			if (log.isDebugEnabled()) {
				log.debug("removeChunk: time = " + tm + " ms.");
			}

			done[0] = isFinished && elementsInCache == 0
					&& (inBuffer == null || inBuffer.getNElements() == 0)
					&& (exBuffer == null || exBuffer.getNElements() == 0);
			return retval;
		}
	}

	public WritableContainer<WritableTuple> getContainer() {
		WritableContainer<WritableTuple> retval = fb.get();
		retval.init(sortingBucket);
		return retval;
	}

	public void releaseContainer(WritableContainer<WritableTuple> v) {
		v.clear();
		fb.release(v);
	}

	private WritableContainer<WritableTuple> removeChunk(boolean[] done)
			throws Exception {
		if (sort) {
			WritableContainer<WritableTuple> retval = sortedBucketCache
					.removeChunk(done);
			if (retval != null) {
				synchronized (this) {
					elementsInCache -= retval.getNElements();
				}
			}
			return retval;
		} else {
			return removeChunkUnsorted(done);
		}
	}

	private synchronized void combineInExBuffers() {
		long time = System.currentTimeMillis();

		if (exBuffer == null || exBuffer.getNElements() == 0) {
			releaseExBuffer();
			return;
		}

		if (inBuffer == null || inBuffer.getNElements() == 0) {
			releaseInBuffer();
			inBuffer = exBuffer;
			isInBufferSorted = isExBufferSorted;
		} else {

			boolean response = false;
			isInBufferSorted = isInBufferSorted && isExBufferSorted;

			if (exBuffer.getNElements() > inBuffer.getNElements()) {
				// LOG-DEBUG
				if (log.isDebugEnabled()) {
					log.debug("combineInExBuffers: adding inBuffer, sorted = "
							+ isInBufferSorted + " with "
							+ inBuffer.getNElements() + " elements "
							+ "TO exBuffer, sorted = " + isExBufferSorted
							+ " with " + exBuffer.getNElements() + " elements");
				}

				response = exBuffer.addAll(inBuffer);

				if (!response) {
					// Cache inBuffer and replace it with exBuffer
					cacheBuffer(inBuffer, isInBufferSorted);
					isInBufferSorted = isExBufferSorted;
				} else {
					releaseInBuffer();
					isInBufferSorted = false;
				}

				// Replace inBuffer with exBuffer
				inBuffer = exBuffer;
			} else {
				// LOG-DEBUG
				if (log.isDebugEnabled()) {
					log.debug("combineInExBuffers: adding exBuffer, sorted = "
							+ isExBufferSorted + " with "
							+ exBuffer.getNElements() + " elements "
							+ "TO inBuffer, sorted = " + isInBufferSorted
							+ " with " + inBuffer.getNElements() + " elements");
				}

				response = inBuffer.addAll(exBuffer);

				if (!response) {
					// Cache exBuffer and reset it afterwards
					cacheBuffer(exBuffer, isExBufferSorted);
					// isInBufferSorted stays what it was.
				} else {
					releaseExBuffer();
					isInBufferSorted = false;
				}
			}
		}

		// Reset exBuffer
		exBuffer = null;
		isExBufferSorted = true;

		stats.addCounter(submissionNode, submissionId,
				"Bucket:combineInExBuffers: overall time (ms)",
				System.currentTimeMillis() - time);
	}

	private synchronized void prepareRemoveWChunk() {
		if (isFillingThreadStarted) {
			return;
		}

		// LOG-DEBUG
		if (log.isDebugEnabled()) {
			log.debug("prepareRemoveWChunk: init variables...");
		}

		isFillingThreadStarted = true;
		ThreadPool.createNew(new Runnable() {
			@Override
			public void run() {
				try {
					fillWriteBuffers();
				} catch (Throwable e) {
					if (log.isDebugEnabled()) {
						log.debug("Got exception in fillWriteBuffers!", e);
					}
					context.killSubmission(submissionNode, submissionId, e);
				}
			}
		}, "FillWriteBuffers for bucket " + key);
	}

	private void fillWriteBuffers() throws Exception {

		// LOG-DEBUG
		if (log.isDebugEnabled()) {
			log.debug("fillWriteBuffers: start the thread for double-buffering...");
		}

		boolean[] retval = new boolean[1];
		WriteBuffer w;
		for (;;) {
			synchronized (freeListLock) {
				while (freeList == null) {
					try {
						freeListLock.wait();
					} catch (InterruptedException e) {
						// ignore
					}
				}
				w = freeList;
				freeList = freeList.next;
			}
			// LOG-DEBUG
			if (log.isDebugEnabled()) {
				log.debug("fillWriteBufers: start filling writeBuffer");
			}

			w.next = null;
			w.buffer = removeChunk(retval);
			w.removeChunkReturned = retval[0];

			synchronized (availableListLock) {
				WriteBuffer b = availableList, prev = null;
				while (b != null) {
					prev = b;
					b = b.next;
				}
				if (prev == null) {
					availableList = w;
				} else {
					prev.next = w;
				}
				availableListLock.notifyAll();
				if (w.removeChunkReturned) {
					// LOG-DEBUG
					if (log.isDebugEnabled()) {
						log.debug("fillWriteBufers: done, no more chunks to fill with, "
								+ "stop the thread for double-buffering & notify all...");
					}
					done = true;
					return;
				}
			}
		}
	}

	// Returns an empty container when the blocking version would block
	// indefinitely.
	public WritableContainer<WritableTuple> nonBlockingRemoveWChunk(
			boolean[] ready) {
		boolean returnNow = true;
		synchronized (this) {
			if (totalNumberOfElements > 0) {
				returnNow = false;
			}
		}
		if (returnNow) {
			synchronized (availableListLock) {
				returnNow = availableList == null && !done;
			}
		}
		if (returnNow) {
			return getContainer();
		}
		return removeWChunk(ready);
	}

	public WritableContainer<WritableTuple> removeWChunk(boolean[] ready) {

		long timeStart = System.currentTimeMillis();
		WritableContainer<WritableTuple> retval = null;
		boolean isf = false;
		WriteBuffer w;

		// LOG-DEBUG
		if (log.isDebugEnabled()) {
			log.debug("removeWChunk: attempt removing write chunks from writeBuffers, bucket = "
					+ key);
		}

		synchronized (availableListLock) {
			while (availableList == null) {
				if (done) {
					if (ready != null) {
						ready[0] = true;
					}
					return getContainer();
				}
				try {
					availableListLock.wait();
				} catch (InterruptedException e) {
					// ignore
				}
			}
			w = availableList;
			availableList = w.next;
		}

		retval = w.buffer;
		w.buffer = null;
		if (retval == null) {
			retval = getContainer();
		}
		synchronized (this) {
			totalNumberOfElements -= retval.getNElements();
		}

		if (w.removeChunkReturned) {
			// LOG-DEBUG
			if (log.isDebugEnabled()) {
				log.debug("removeWChunk: done, no more chunks to remove, bucket = "
						+ key);
			}

			stats.addCounter(submissionNode, submissionId,
					"Bucket:removeWChunk: overall time (ms)",
					System.currentTimeMillis() - timeStart);
			if (ready != null) {
				ready[0] = true;
			}
			return retval;
		}
		isf = w.removeChunkReturned;

		// LOG-DEBUG
		if (log.isDebugEnabled()) {
			log.debug("removeWChunk for bucket " + key + ": added "
					+ retval.getNElements() + " tuples to retval");
		}
		synchronized (freeListLock) {
			w.next = freeList;
			freeList = w;
			if (!startedReading) {
				startedReading = true;
				for (int i = 1; i < N_WBUFFS; i++) {
					// Already created on in init().
					// Now that we are actually reading, create the rest.
					w = new WriteBuffer();
					w.next = freeList;
					freeList = w;
				}
			}
			freeListLock.notifyAll();
		}

		stats.addCounter(submissionNode, submissionId,
				"Bucket:removeWChunk: overall time (ms)",
				System.currentTimeMillis() - timeStart);
		if (ready != null) {
			ready[0] = isf;
		}
		return retval;
	}

	/**
	 * Method to set the bucket's 'finished' flag.
	 */
	public synchronized void setFinished() {

		// Combine internal + external buffers before finish
		combineInExBuffers();

		if (sort) {
			if (inBuffer != null) {
				if (!isInBufferSorted) {
					TupleComparator c = new TupleComparator();
					comparator.copyTo(c);
					try {
						inBuffer.sort(c, fb);
					} catch (Exception e) {
						throw new Error("Unexpected exception", e);
					}
				}
				elementsInCache += inBuffer.getNElements();
			}
			sortedBucketCache.addContainer(inBuffer);
			inBuffer = null;
			waitForCachersToFinish();
		}

		if (notifier != null) {
			notifier.markReady(iter);
			notifier = null;
			iter = null;
		}

		isFinished = true;

		// Notify possible waiters.
		notifyAll();

		if (!isFillingThreadStarted)
			prepareRemoveWChunk();
	}

	/**
	 * Method to update counters about the send-receive status (if last
	 * sequence/chunk was received or not).
	 * 
	 * @param sequence
	 *            Sequence number
	 * @param lastSequence
	 *            True/false, if is the last received sequence or not
	 */
	public synchronized void updateCounters(int sequence, boolean lastSequence) {

		if (log.isDebugEnabled()) {
			log.debug("updateCounters of bucket: " + this.key + ", sequence = "
					+ sequence + ", lastSequence = " + lastSequence);
		}

		if (sequence >= sequencesReceived.length) {
			sequencesReceived = Arrays.copyOf(sequencesReceived,
					2 * sequencesReceived.length);
		}
		sequencesReceived[sequence]++;

		if (highestSequence < sequence) {
			highestSequence = sequence;
		}

		if (lastSequence) {
			nBucketReceived++;
			for (int i = 0; i < sequence + 1; ++i) {
				sequencesReceived[i]--;
			}
		}

		checkFinished();
	}

	/**
	 * This method is used to update counters with information about the current
	 * chain status.
	 * 
	 * @param idChain
	 *            Current chain id
	 * @param idParentChain
	 *            Current chain's parent id
	 * @param nchildren
	 *            Number of children that the chain have
	 * @param isResponsible
	 *            True/false, whether the current chain is a root chain or not
	 */
	public synchronized void updateCounters(long idChain, long idParentChain,
			int nchildren, boolean isResponsible) {

		if (log.isDebugEnabled()) {
			log.debug("Update counters of bucket " + this.key + ": ic "
					+ idChain + " p " + idParentChain + " c " + nchildren + " "
					+ isResponsible);
		}

		if (nchildren > 0) { // Set the expected children in the
			// map
			Integer c = children.get(idChain);
			if (c == null) {
				children.put(idChain, nchildren);
			} else {
				c += nchildren;
				if (c == 0) {
					children.remove(idChain);
				} else {
					children.put(idChain, c);
				}
			}

		}

		if (isResponsible) { // It is a root chain
			receivedMainChain = true;
		} else {
			Integer c = children.get(idParentChain);
			if (c == null) {
				children.put(idParentChain, -1);
			} else {
				c--;
				if (c == 0) {
					children.remove(idParentChain);
				} else {
					children.put(idParentChain, c);
				}
			}
		}

		nChainsReceived++;

		checkFinished();
	}

	/**
	 * This method is used to wait until all the cachers (the threads that
	 * caches into files the content of a buffer) have finished spilling to disk
	 * -- calling notify.
	 */
	private synchronized void waitForCachersToFinish() {
		numWaitingForCachers++;

		while (numCachers > 0) {
			if (log.isDebugEnabled()) {
				log.debug("Waiting for cachers: " + numCachers);
			}

			try {
				wait();
			} catch (InterruptedException e) {
				// ignore
			}
		}

		numWaitingForCachers--;
	}

	/**
	 * Method used to enter in wait() until the bucket is marked/flagged as
	 * being finished (the last sequence of tuples / last chunk was
	 * transferred).
	 * 
	 * @return True, when the bucket is finished otherwise we enter into wait()
	 *         mode
	 */
	public synchronized boolean waitUntilFinished() {
		while (!isFinished) {
			if (log.isDebugEnabled()) {
				log.debug("waitUntilFinished on bucket " + this.key);
			}
			try {
				wait();
			} catch (InterruptedException e) {
				// ignore
			}
			if (log.isDebugEnabled()) {
				log.debug("waitUntilFinished on bucket " + this.key + " done");
			}
		}

		return true;
	}

	public byte[] getSignature() {
		return signature;
	}

	public TupleComparator getComparator() {
		return comparator;
	}

	public synchronized void setAdditionalCounters(long[] additionalChains,
			int[][] additionalValues) {

		if (additionalChildrenCounts == null) {
			additionalChildrenCounts = new HashMap<Long, List<Integer>>();
		}

		for (int i = 0; i < additionalChains.length; ++i) {
			List<Integer> values = additionalChildrenCounts
					.get(additionalChains[i]);
			if (values == null) {
				values = new ArrayList<Integer>();
				additionalChildrenCounts.put(additionalChains[i], values);
			}

			for (int j : additionalValues[i]) {
				values.add(j);
			}
		}
	}

	public synchronized void setAdditionalCounters(
			Map<Long, List<Integer>> additionalCounters) {

		if (additionalChildrenCounts == null) {
			additionalChildrenCounts = new HashMap<Long, List<Integer>>();
		}

		for (Map.Entry<Long, List<Integer>> value : additionalCounters
				.entrySet()) {
			List<Integer> existingValue = additionalChildrenCounts.get(value
					.getKey().longValue());
			if (existingValue == null) {
				additionalChildrenCounts.put(value.getKey().longValue(),
						value.getValue());
			} else {
				existingValue.addAll(value.getValue());
			}
		}
	}

	public Map<Long, List<Integer>> getAdditionalChildrenCounts() {
		return additionalChildrenCounts;
	}

	public boolean isSorted() {
		return sort;
	}

	public boolean hasData() {
		return availableToTransmit();
	}

	public WritableTuple getSerializer() {
		return new WritableTuple(serializer);
	}

	public boolean isSortingBucket() {
		return sortingBucket;
	}
}
