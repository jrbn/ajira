package nl.vu.cs.ajira.buckets;

import ibis.util.ThreadPool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
// import org.xerial.snappy.SnappyInputStream;
// import org.xerial.snappy.SnappyOutputStream;

/**
 * This class represents the data structure used to store tuples in the main
 * memory and to manage caching/storing on the disk when the size exceeds the
 * maximum limit. Basically, it contains a buffer and a map over the files that
 * were used to spill tuples on the disk.
 */
public class Bucket {

	/**
	 * Keeps information about the content (tuples) cached/stored on the disk,
	 * such as where it was stored (filename), the last element written in the
	 * file (lastElement), total elements (nElements), how much space remained
	 * in the file (remaining size) and the file stream -- file descriptor.
	 */
	static class FileMetaData {
		String filename;
		byte[] lastElement;
		long nElements;
		long remainingSize;
		FDataInput stream;

		public void finished() {
			try {
				stream.close();
			} catch (IOException e) {
				// ignore
			}
			new File(filename).delete();
		}
	}

	/**
	 * This class represents an implementation of a sorted list, with a custom
	 * comparator, used for merging the elements from the main memory (buffer)
	 * with the ones from the files stored on the disk (cacheFiles).
	 * 
	 * @param <T>
	 */
	static class SortedList<T> extends ArrayList<T> {
		private static final long serialVersionUID = 919780998989097240L;
		private final Comparator<T> comparator;

		public SortedList(int capacity, Comparator<T> comparator) {
			super(capacity);
			this.comparator = comparator;
		}

		/**
		 * Adds this element to the list at the proper sorting position.
		 */
		@Override
		public synchronized boolean add(T e) {
			if (size() == 0) {
				return super.add(e);
			}

			int index = Collections.binarySearch(this, e, comparator);

			if (index < 0) {
				index = -index - 1;
			}

			super.add(index, e);

			return true;
		}

		public T getLastElement() {
			return get(size() - 1);
		}

		public T removeLastElement() {
			return remove(size() - 1);
		}
	}
	
	public static final int N_WBUFFS = 2;
	
	private static class WriteBuffer {
		WritableContainer<WritableTuple> buffer;
		boolean removeChunkReturned;
		WriteBuffer next;
		public boolean done;
	}
	
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

	private Factory<WritableContainer<WritableTuple>> fb = null;
	boolean gettingData;
	private int highestSequence;

	private boolean isFinished;

	private TupleIterator iter;
	private long key;
	private CachedFilesMerger merger;

	private Map<Long, List<Integer>> additionalChildrenCounts = null;

	SortedList<byte[]> minimumSortedList = new SortedList<byte[]>(100,
			new Comparator<byte[]>() {
				@Override
				public int compare(byte[] o1, byte[] o2) {
					return -comparator.compare(o1, 0, o1.length, o2, 0,
							o2.length);
				}
			});

	private int nBucketReceived = 0;
	private int nChainsReceived = 0;

	private ChainNotifier notifier;

	int numCachers;
	int numWaitingForCachers;
	Map<byte[], FileMetaData> sortedCacheFiles = new HashMap<byte[], FileMetaData>();

	private boolean receivedMainChain;

	private final byte[] sequencesReceived = new byte[Consts.MAX_SEGMENTS_RECEIVED];

	private StatisticsCollector stats;
	private int submissionId;
	private int submissionNode;
	// Internal tuples - assigned tuples read from local files
	private WritableContainer<WritableTuple> inBuffer = null;
	// External tuples - assigned tuples pulled from remote files
	private WritableContainer<WritableTuple> exBuffer = null;
	private boolean isInBufferSorted = true;
	private boolean isExBufferSorted = true;

	private boolean isFillingThreadStarted;
	private int waitersForAdditions;

	private boolean hasData;
	private final Object lockHasData = new Object();

	private long totalNumberOfElements = 0;
	
	boolean sortingBucket;

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
			inBuffer = fb.get();
			inBuffer.init(sortingBucket);
		}
		totalNumberOfElements++;

		serializer.setTuple(tuple);
		boolean response = inBuffer.add(serializer);

		if (response) {
			isInBufferSorted = inBuffer.getNElements() < 2;
		} else {
			cacheBuffer(inBuffer, isInBufferSorted);
			inBuffer = fb.get();
			inBuffer.init(sortingBucket);
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

			if (waitersForAdditions > 0) {
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
	 * This method may reuse the newTuplesContainer, so after calling this method,
	 * you can no longer use the container.
	 * 
	 * @param newTuplesContainer
	 *            New buffer that has to be inserted
	 * @param isSorted
	 *            True/false if whether the tuples from inside the container are
	 *            sorted or not
	 * @throws Exception
	 */
	public synchronized void addAll(WritableContainer<WritableTuple> newTuplesContainer,
			boolean isSorted)
					throws Exception {
		totalNumberOfElements += newTuplesContainer.getNElements();

		long time = System.currentTimeMillis();

		if (exBuffer == null) {
			exBuffer = fb.get();
			exBuffer.init(sortingBucket);
			isExBufferSorted = true;
		}

		// LOG-DEBUG
		if (log.isDebugEnabled()) {
			log.debug("addAll: adding a sorted = " + isSorted
					+ " buffer with " + newTuplesContainer.getNElements()
					+ " elements " + "TO exBuffer, sorted = "
					+ isExBufferSorted + " with " + exBuffer.getNElements()
					+ " elements");
		}

		boolean isExBufferEmpty = (exBuffer.getNElements() == 0);
		boolean response = false;

		if (isExBufferEmpty) {
			releaseExBuffer();
			exBuffer = newTuplesContainer;
			isExBufferSorted = isSorted;
			response = true;
		} else {
			if (newTuplesContainer.getNElements() > exBuffer.getNElements()) {
				response = newTuplesContainer.addAll(exBuffer);

				if (response) {
					releaseExBuffer();
					exBuffer = newTuplesContainer;
				}
			} else {
				response = exBuffer.addAll(newTuplesContainer);
				if (response) {
					newTuplesContainer.clear();
					fb.release(newTuplesContainer);
				}
			}

			isExBufferSorted = (response && isSorted && isExBufferEmpty)
					|| (isExBufferSorted && !response);
		}

		if (!response && !isExBufferEmpty) {
			// The response is 'false', which means there is not much space
			// left in exBuffer
			if (exBuffer.getNElements() > newTuplesContainer.getNElements()) {
				// Cache exBuffer to make space
				cacheBuffer(exBuffer, isExBufferSorted);
				// Replace exBuffer with the new container
				exBuffer = newTuplesContainer;
				isExBufferSorted = isSorted;
			} else {
				// It's better to store the other buffer
				// We can just use the other container
				cacheBuffer(newTuplesContainer, isSorted);
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
				"Bucket:addAll: overall time (ms)",
				System.currentTimeMillis() - time);
	}

	/**
	 * Method that checks if there are tuples available to be transferred.
	 * 
	 * @return true/false whether the bucket has enough bytes to transfer or
	 *         there are some cached files and we prefer to empty the buffer
	 */
	public synchronized boolean availableToTransmit() {
		return elementsInCache > 0
				|| (inBuffer != null && inBuffer.getRawSize() > Consts.MIN_SIZE_TO_SEND);
	}
	

	/**
	 * Method that checks if there are tuples available to be transferred in streaming mode.
	 * 
	 * @return true/false whether the bucket has enough bytes to transfer or
	 *         there are some cached files and we prefer to empty the buffer
	 */
	public synchronized boolean availableToTransmitWhileStreaming() {
		return elementsInCache > 0
				|| (inBuffer != null && inBuffer.getRawSize() > 0);
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
	 * @throws IOException
	 */
	private void cacheBuffer(final WritableContainer<WritableTuple> buffer,
			final boolean sorted)
					throws IOException {
		if (buffer.getNElements() == 0) {
			buffer.clear();
			fb.release(buffer);
			return;
		}
		
		final Throwable ex = log.isDebugEnabled() ? new Throwable() : null;

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

					File cacheFile = File.createTempFile("cache", "tmp");
					cacheFile.deleteOnExit();

					OutputStream fout = new SnappyOutputStream(
							new BufferedOutputStream(new FileOutputStream(
									cacheFile), 65536));

					FDataOutput cacheOutputStream = new FDataOutput(fout);

					long time = System.currentTimeMillis();
					if (!sort) {
						buffer.writeTo(cacheOutputStream);
					} else {
						buffer.writeElementsTo(cacheOutputStream);
						cacheOutputStream.writeInt(0);
					}

					stats.addCounter(submissionNode, submissionId,
							"Time spent writing to cache (ms)",
							System.currentTimeMillis() - time);

					cacheOutputStream.close();

					// Register file in the list of cachedBuffers
					synchronized (Bucket.this) {
						if (!sort) {
							cacheFiles.add(cacheFile);
						} else {
							FDataInput is = new FDataInput(
									new SnappyInputStream(
											new BufferedInputStream(
													new FileInputStream(
															cacheFile), 65536)));
							// Read the first element and put it into the map
							try {
								int length = is.readInt();
								byte[] rawValue = new byte[length];
								is.readFully(rawValue);

								FileMetaData meta = new FileMetaData();
								meta.filename = cacheFile.getAbsolutePath();
								meta.stream = is;
								meta.nElements = buffer.getNElements() - 1;
								meta.lastElement = buffer.getLastElement();
								if (log.isDebugEnabled()) {
									log.debug("Size of first element is "
											+ length
											+ ", size of last element is "
											+ meta.lastElement.length);
								}
								meta.remainingSize = buffer.getRawSize() - 4
										- length;

								sortedCacheFiles.put(rawValue, meta);
								minimumSortedList.add(rawValue);
							} catch (Throwable e) {
								log.error("Error", e);
								if (ex != null) {
									log.error("Call came from ", ex);
								}
							}

							if (sortedCacheFiles.size() > 8) {
								merger.newRequest(Bucket.this);
							}
						}
					}
					buffer.clear();
					fb.release(buffer);
				} catch (IOException e) {
					// TODO: what to do now?
					log.error("Got exception while writing cache!", e);
				}
				synchronized (Bucket.this) {
					numCachers--;
					if (numCachers == 0 && numWaitingForCachers > 0) {
						Bucket.this.notifyAll();
					}
				}
			}
		}, "Sort-and-cache");

	}

	/**
	 * This method is used to internally check if all the send-receive
	 * operations performed over the bucket are finished.
	 * 
	 * @throws IOException
	 */
	private void checkFinished() throws IOException {

		if (log.isDebugEnabled()) {
			log.debug("checkFinished " + this.key + ": nChainsReceived = " + nChainsReceived
					+ ", nBucketReceived = " + nBucketReceived
					+ ", highestSequence = " + highestSequence
					+ ", children = " + children.toString()
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

	/**
	 * This method is used to check whether the element given as a parameter is
	 * less/greater then the last element (min) from the sorted list used for
	 * merging the bucket's tuples.
	 * 
	 * @param element
	 *            The element (sequence of bytes) to compare with
	 * @return True/false, if the element is lees or not than the sorted list's
	 *         minimum
	 */
	private boolean compareWithSortedList(byte[] element) {
		return minimumSortedList.size() == 0
				|| (element != null && minimumSortedList.comparator.compare(
						element, minimumSortedList.getLastElement()) >= 0);
	}

	/**
	 * This method is used to copy an entire file from the disk, which contains
	 * cached sorted tuples, to a buffer, if the minimum element from the file
	 * is less then the minimum element from the sorted list.
	 * 
	 * @param meta
	 *            The meta information about the file that is checked if can
	 *            copied into the buffer
	 * @param tmpBuffer
	 *            The destination buffer - where to copy the entire cached file
	 *            if the conditions are passed
	 * @param minimum
	 *            The minimum element from the file which we attempt to copy
	 *            into the buffer (if the conditions are fulfilled)
	 * @return True/false if the copy attempt was successful or not
	 * @throws Exception
	 */
	private boolean copyFullFile(FileMetaData meta,
			WritableContainer<WritableTuple> tmpBuffer, byte[] minimum)
			throws Exception {
		// Check whether the last element is smaller than the second minimum.
		// If it is, then we can copy the entire file in the buffer.
		if (compareWithSortedList(meta.lastElement)) {
			// Copy the entire file in the buffer
			if (tmpBuffer.addAll(meta.stream, meta.lastElement, meta.nElements,
					meta.remainingSize)) {
				elementsInCache -= meta.nElements;
				meta.finished();
				sortedCacheFiles.remove(minimum);

				return true;
			}
		}
		return false;
	}

	public long getKey() {
		return key;
	}

	public boolean shouldSort() {
		return sort;
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
	void init(long key, StatisticsCollector stats, int submissionNode,
			int submissionId, boolean sort, boolean sortRemote,
			byte[] sortingFields, Factory<WritableContainer<WritableTuple>> fb,
			CachedFilesMerger merger, byte[] signature) {
		this.sortingBucket = sort || sortRemote;
		this.key = key;
		this.fb = fb;
		this.inBuffer = null;
		this.merger = merger;

		isFinished = false;
		receivedMainChain = false;
		nChainsReceived = 0;
		nBucketReceived = 0;
		highestSequence = -1;
		gettingData = false;
		waitersForAdditions = 0;
		hasData = false;

		notifier = null;
		iter = null;

		elementsInCache = 0;

		sortedCacheFiles.clear();
		minimumSortedList.clear();
		cacheFiles.clear();
		children.clear();

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
		} else if (sortRemote) {
			this.serializer = new WritableTuple(sortingFields, signature.length);
		} else {
			this.serializer = new WritableTuple();
		}
		for (int i = 0; i < N_WBUFFS; i++) {
			WriteBuffer w = new WriteBuffer();
			w.next = freeList;
			freeList = w;
		}
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

			return (inBuffer.getTotalCapacity() + exBuffer
					.getTotalCapacity());
		}
	}
	
	public synchronized boolean isEmpty() {
		if (log.isDebugEnabled()) {
			log.debug("isEmpty() on bucket " + key + ": totalNumberOfElements = " + totalNumberOfElements);
		}
		return totalNumberOfElements == 0;

	}

	boolean isFinished() {
		synchronized(lockHasData) {
			return isFinished;
		}
	}

	/**
	 * Method used to read from a cached file into a buffer.
	 * 
	 * @param meta
	 *            Meta information about he source file
	 * @param buf
	 *            The destination buffer
	 * @throws IOException
	 */
	private void readFrom(FileMetaData meta, byte[] buf) throws IOException {
		meta.stream.readFully(buf);
		meta.remainingSize -= buf.length + 4;
		meta.nElements--;
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
		releaseInBuffer();
		releaseExBuffer();
		// Also remove files.
		for (FileMetaData f : sortedCacheFiles.values()) {
			f.finished();
		}
	}

	// TODO: should be part of WritableContainer I think
	private synchronized void releaseExBuffer() {
		if (exBuffer != null) {
			exBuffer.clear();
			fb.release(exBuffer);
			exBuffer = null;
		}
	}

	// TODO: should be part of WritableContainer I think
	private synchronized void releaseInBuffer() {
		if (inBuffer != null) {
			inBuffer.clear();
			fb.release(inBuffer);
			inBuffer = null;
		}
	}

	/**
	 * Probably the most important method of this class. This method is used to
	 * remove a chunk from the buffer in order to be transferred or written to
	 * the final result file (by calling the bucketIterator). To remove a chunk
	 * and to still have an ordered sequence of tuples we first have to merge
	 * tuples from the buffer with tuples from content cached in files (if any)
	 * up until we managed to fill the chunk to its maximum size.
	 * 
	 * @param tmpBuffer
	 *            The chunk removed from the bucket (globally sorted)
	 * @return True/false, depending on whether the bucket still contains tuples
	 *         to remove (buffer + files) or not
	 * @throws Exception 
	 */

	private synchronized boolean removeChunkSorted(
			WritableContainer<WritableTuple> tmpBuffer) throws Exception {
		// If some threads still have to finish writing
		waitForCachersToFinish();

		if (log.isDebugEnabled()) {
			log.debug("removeChunk: fill tmpBuffer with triples from bucket "
					+ this.getKey());
		}

		long totTime = System.currentTimeMillis();
		gettingData = true;

		try {
			if (!isInBufferSorted) {
				inBuffer.sort(comparator, fb);
				isInBufferSorted = true;
			}

			if (elementsInCache > 0) {
				tmpBuffer.setFieldsDelimiter(true);

				if (log.isDebugEnabled()) {
					log.debug("Try add the first triple from the in-memory ds to the pool => "
							+ "inBuffer.getNElements() = "
							+ inBuffer.getNElements()
							+ ", "
							+ "minmumSortedlist.size() = "
							+ minimumSortedList.size());
				}

				// Add the first triple from the in-memory ds to the
				// pool
				if (inBuffer.getNElements() > 0
						&& minimumSortedList.size() == sortedCacheFiles
						.size()) {
					byte[] key = inBuffer.removeRaw(null);
					minimumSortedList.add(key);

					if (log.isDebugEnabled()) {
						log.debug("First triple from the in-memory ds was added to the pool.");
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("First triple from the in-memory ds was NOT added to the pool.");
					}
				}

				if (log.isDebugEnabled()) {
					log.debug("Sorting bucket: number of streams is "
							+ sortedCacheFiles.size());
				}

				boolean insertResponse = false;
				int tuplesFromBuffer = 0;
				int tuplesFromStream = 0;
				long time = System.currentTimeMillis();
				do {
					// Remove the minimum of the tuples and try to add
					// it to
					// the buffer.
					byte[] minimum = minimumSortedList.removeLastElement();

					insertResponse = tmpBuffer.addRaw(minimum);

					if (insertResponse) {
						if (sortedCacheFiles.containsKey(minimum)) {
							tuplesFromStream++;
							elementsInCache--;
							// The minimum came from a file. Check if
							// the
							// file can be copied completely.
							FileMetaData meta = sortedCacheFiles
									.get(minimum);

							if (copyFullFile(meta, tmpBuffer, minimum)) {
								tuplesFromStream += meta.nElements;
								continue;
							}

							// No, it could not. Now try to stay with
							// this
							// file as long as we can.
							try {
								int length;
								while ((length = meta.stream.readInt()) == minimum.length) {
									// Reuse minimum
									readFrom(meta, minimum);
									if (compareWithSortedList(minimum)
											&& (insertResponse = tmpBuffer
											.addRaw(minimum))) {
										tuplesFromStream++;
										elementsInCache--;
									} else {
										minimumSortedList.add(minimum);
										break;
									}
								}
								// We get here if the length is
								// different
								// (in which case we still have to read
								// the tuple, in a new buffer), or when
								// the
								// order was wrong.
								if (length != minimum.length) {
									sortedCacheFiles.remove(minimum);
									if (length > 0) {
										byte[] rawValue = new byte[length];
										readFrom(meta, rawValue);
										minimumSortedList.add(rawValue);
										sortedCacheFiles
										.put(rawValue, meta);
									} else { // File is finished.
										meta.finished();
									}
								}
							} catch (Exception e) {
								log.warn("Here it should never come!");
								sortedCacheFiles.remove(minimum);
								meta.finished();
							}
						} else { // It came from the in-memory
							// container.
							if (inBuffer.getNElements() > 0
									&& elementsInCache > 0) {
								byte[] key = inBuffer.removeRaw(minimum);
								minimumSortedList.add(key);
							}
							tuplesFromBuffer++;
						}
					} else {
						// Put it back
						minimumSortedList.add(minimum);
					}
				} while (insertResponse && elementsInCache > 0);

				if (log.isDebugEnabled()) {
					log.debug("Tuples from buffer:" + tuplesFromBuffer
							+ " from stream: " + tuplesFromStream
							+ " time: "
							+ (System.currentTimeMillis() - time));
				}
			}

			if (elementsInCache == 0) {
				if (minimumSortedList.size() > 0) {
					// There cannot be more than 1 here
					byte[] minimum = minimumSortedList
							.remove(minimumSortedList.size() - 1);
					if (!tmpBuffer.addRaw(minimum)) {
						minimumSortedList.add(minimum);
					}
				}

				if (inBuffer != null && inBuffer.getNElements() > 0) {
					if (tmpBuffer.addAll(inBuffer)) {
						inBuffer.clear();
					}
				}
			}

		} catch (Exception e) {
			log.error("Error in retrieving the results", e);
			throw e;
		}

		long tm = System.currentTimeMillis() - totTime;
		if (log.isDebugEnabled()) {
			log.debug("removeChunk: time = " + tm + " ms.");
		}

		return elementsInCache == 0
				&& (inBuffer == null || inBuffer.getNElements() == 0)
				&& (sortedCacheFiles == null || minimumSortedList.size() == 0);
	}

	private synchronized boolean removeChunkUnsorted(
			WritableContainer<WritableTuple> tmpBuffer) throws Exception {

		long totTime = System.currentTimeMillis();

		for (;;) {
			// If some threads still have to finish writing
			waitForCachersToFinish();
			try {
				if (elementsInCache > 0) {
					long time = System.currentTimeMillis();
					File fi = null;

					if (cacheFiles.size() > 0) {
						fi = cacheFiles.remove(0);
					}

					if (fi != null) {
						FDataInput di = new FDataInput(new SnappyInputStream(
								new BufferedInputStream(new FileInputStream(fi),
										65536)));
						tmpBuffer.readFrom(di); // Read the oldest file
						stats.addCounter(submissionNode, submissionId,
								"Bucket:removeChunk: time reading from cache (ms)",
								System.currentTimeMillis() - time);
						stats.addCounter(submissionNode, submissionId,
								"Bucket:removeChunk: Bytes read from cache",
								tmpBuffer.getRawSize());
						elementsInCache -= tmpBuffer.getNElements();
						di.close();
						fi.delete();
					}
				}

				// Let's see if we can also get the data either from the
				// inBuffer, or exBuffer, but then we must be sure they are not
				// being cached.
				if (tmpBuffer.getNElements() == 0) {
					if (inBuffer != null) {
						if (tmpBuffer.addAll(inBuffer)) {
							inBuffer.clear();
						}

						// Ok, now we try also exBuffer...
						if (tmpBuffer.getNElements() == 0) {
							if (exBuffer != null) {
								if (tmpBuffer.addAll(exBuffer)) {
									exBuffer.clear();
								}
							}
						}
					}
				}

				// There was nothing available. Now we do not have any choice than
				// wait.

				if (tmpBuffer.getNElements() == 0) {
					boolean isBucketFinished;
					synchronized(lockHasData) {
						isBucketFinished = isFinished;
					}
					if (! isBucketFinished) {
						waitersForAdditions++;
						this.wait();
						waitersForAdditions--;

						// It became finished while I was executing the previous
						// code. Let's try again.
						continue;
					}
					return true;
				}
			} catch (Exception e) {
				log.error("Error in retrieving the results", e);
				throw e;
			}

			long tm = System.currentTimeMillis() - totTime;
			if (log.isDebugEnabled()) {
				log.debug("removeChunk: time = " + tm + " ms.");
			}

			synchronized(lockHasData) {
				return isFinished && elementsInCache == 0
						&& (inBuffer == null || inBuffer.getNElements() == 0)
						&& (exBuffer == null || exBuffer.getNElements() == 0);
			}
		}
	}

	private boolean removeChunk(WritableContainer<WritableTuple> tmpBuffer) throws Exception {
		if (sort) {
			return removeChunkSorted(tmpBuffer);
		} else {
			return removeChunkUnsorted(tmpBuffer);
		}
	}

	private synchronized void combineInExBuffers() throws Exception {
		long time = System.currentTimeMillis();

		if (exBuffer == null || exBuffer.getNElements() == 0) {
			return;
		}

		if (inBuffer == null) {
			inBuffer = fb.get();
			inBuffer.init(sortingBucket);
			isInBufferSorted = true;
		}

		boolean response = false;
		isInBufferSorted = isInBufferSorted && isExBufferSorted;

		if (exBuffer.getNElements() > inBuffer.getNElements()) {
			// LOG-DEBUG
			if (log.isDebugEnabled()) {
				log.debug("combineInExBuffers: adding inBuffer, sorted = "
						+ isInBufferSorted
						+ " with "
						+ inBuffer.getNElements()
						+ " elements "
						+ "TO exBuffer, sorted = "
						+ isExBufferSorted
						+ " with "
						+ exBuffer.getNElements()
						+ " elements");
			}

			response = exBuffer.addAll(inBuffer);

			if (!response) {
				// Cache inBuffer and replace it with exBuffer
				cacheBuffer(inBuffer, isInBufferSorted);
				isInBufferSorted = isExBufferSorted;
			} else {
				isInBufferSorted = false;
			}

			// Replace inBuffer with exBuffer
			inBuffer = exBuffer;
		} else {
			// LOG-DEBUG
			if (log.isDebugEnabled()) {
				log.debug("combineInExBuffers: adding exBuffer, sorted = "
						+ isExBufferSorted
						+ " with "
						+ exBuffer.getNElements()
						+ " elements "
						+ "TO inBuffer, sorted = "
						+ isInBufferSorted
						+ " with "
						+ inBuffer.getNElements()
						+ " elements");
			}

			response = inBuffer.addAll(exBuffer);

			if (!response) {
				// Cache exBuffer and reset it afterwards
				cacheBuffer(exBuffer, isExBufferSorted);
			} else {
				// Oops, this was missing! --Ceriel
				isInBufferSorted = false;
			}
		}

		// Reset exBuffer
		exBuffer = fb.get();
		exBuffer.init(sortingBucket);

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
				} catch(Exception e) {
					log.error("Got exception in fillWriteBuffers!", e);
				}
			}
		}, "FillWriteBuffers");
	}

	private void fillWriteBuffers() throws Exception {

		// LOG-DEBUG
		if (log.isDebugEnabled()) {
			log.debug("fillWriteBuffers: start the thread for double-buffering...");
		}

		WriteBuffer w;
		for (;;) {
			synchronized(freeListLock) {
				while (freeList == null) {
					try {
						freeListLock.wait();
					} catch(Throwable e) {
						// ignore
					}
				}
				w = freeList;
				freeList = freeList.next;
			}
			if (w.buffer == null) {
				w.buffer = fb.get();
				w.buffer.init(sortingBucket);
			}
			// LOG-DEBUG
			if (log.isDebugEnabled()) {
				log.debug("fillWriteBufers: start filling writeBuffer");
			}
			w.done = false;
			w.next = null;
			w.removeChunkReturned = removeChunk(w.buffer);

			synchronized(lockHasData) {
				if (!isFinished) {
					hasData = true;
				}
			}
			
			synchronized(availableListLock) {
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
				if (w.buffer.getNElements() == 0) {
					// LOG-DEBUG
					if (log.isDebugEnabled()) {
						log.debug("fillWriteBufers: done, no more chunks to fill with, "
								+ "stop the thread for double-buffering & notify all...");
					}
					w.done = true;
					return;
				}
			}
		}
	}

	public WritableContainer<WritableTuple> removeWChunk(boolean[] ready) {

		long timeStart = System.currentTimeMillis();
		WritableContainer<WritableTuple> retval = null;
		boolean isf = false;
		WriteBuffer w;

		// LOG-DEBUG
		if (log.isDebugEnabled()) {
			log.debug("removeWChunk: attempt removing write chunks from writeBuffers");
		}

		synchronized(availableListLock) {
			while (availableList == null) {
				try {
					availableListLock.wait();
				} catch(Throwable e) {
					// ignore
				}
			}
			w = availableList;
			availableList = w.next;
		}

		if (w.done) {
			// LOG-DEBUG
			if (log.isDebugEnabled()) {
				log.debug("removeWChunk: done, no more chunks to remove");
			}

			stats.addCounter(submissionNode, submissionId,
					"Bucket:removeWChunk: overall time (ms)",
					System.currentTimeMillis() - timeStart);
			if (ready != null) {
				ready[0] = false;
			}
			w.buffer.clear();
			fb.release(w.buffer);
			synchronized(freeListLock) {
				while (freeList != null) {
					if (freeList.buffer != null) {
						freeList.buffer.clear();
						fb.release(freeList.buffer);
						freeList.buffer = null;
					}
					freeList = freeList.next;
				}
			}
			return new WritableContainer<WritableTuple>(1);
		}

		retval = w.buffer;
		synchronized(this) {
			totalNumberOfElements -= retval.getNElements();
		}
		w.buffer = fb.get();
		w.buffer.init(sortingBucket);
		isf = w.removeChunkReturned;

		// LOG-DEBUG
		if (log.isDebugEnabled()) {
			log.debug("removeWChunk: added " + retval.getNElements()
					+ " tuples to retval");
		}
		synchronized(freeListLock) {
			w.next = freeList;
			freeList = w;
			freeListLock.notifyAll();
		}

		synchronized(lockHasData) {
			if (!this.isFinished) {
				synchronized(availableListLock) {
					if (availableList == null
							|| (availableList.buffer.getNElements() == 0 && !availableList.done)) {
						hasData = false;
					}
				}
			}
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

	 * @throws IOException
	 */
	public synchronized void setFinished() throws IOException {
		// Combine internal + external buffers before finish
		try {
			combineInExBuffers();
		} catch (Throwable e) {
			log.error("got execption", e);
		}
		if (notifier != null) {
			notifier.markReady(iter);
			notifier = null;
			iter = null;
		}
		synchronized(lockHasData) {
			isFinished = true;
			lockHasData.notifyAll();
		}

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
	 * @throws IOException
	 */
	public synchronized void updateCounters(int sequence, boolean lastSequence)
			throws IOException {

		if (log.isDebugEnabled()) {
			log.debug("updateCounters of bucket: " + this.key + ", sequence = "
					+ sequence + ", lastSequence = " + lastSequence);
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
	 * @throws IOException
	 */
	public synchronized void updateCounters(long idChain, long idParentChain,
			int nchildren, boolean isResponsible) throws IOException {

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
	public boolean waitUntilFinished() {
		synchronized(lockHasData) {
			while (!isFinished) {
				if (log.isDebugEnabled()) {
					log.debug("waitUntilFinished on bucket " + this.key);
				}
				try {
					lockHasData.wait();
				} catch(Throwable e) {
					// ignore
				}
				if (log.isDebugEnabled()) {
					log.debug("waitUntilFinished on bucket " + this.key
							+ " done");
				}
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
		// return (inBuffer != null && inBuffer.getNElements() > 0)
		// || (exBuffer != null && exBuffer.getNElements() > 0)
		// || elementsInCache > 0;
		synchronized(lockHasData) {
			return hasData;
		}
	}
	
	public WritableTuple getSerializer() {
		return new WritableTuple(serializer);
	}
}
