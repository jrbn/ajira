package arch.buckets;

import ibis.util.ThreadPool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import arch.chains.ChainNotifier;
import arch.data.types.Tuple;
import arch.data.types.bytearray.FDataInput;
import arch.data.types.bytearray.FDataOutput;
import arch.datalayer.TupleIterator;
import arch.statistics.StatisticsCollector;
import arch.storage.Factory;
import arch.storage.RawComparator;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class Bucket {

	public static class FileMetaData {
		String filename;
		FDataInput stream;
		byte[] lastElement;
		long nElements;
		long remainingSize;
	}

	static class SortedList<T> extends ArrayList<T> {

		/**
		 * 
		 */
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

		public T removeLastElement() {
			return remove(size() - 1);
		}

		public T getLastElement() {
			return get(size() - 1);
		}
	}

	static final Logger log = LoggerFactory.getLogger(Bucket.class);

	private long key;
	// Internal tuples - assigned tuples read from local files
	private WritableContainer<Tuple> inBuffer = null;
	// External tuples - assigned tuples pulled from remote files
	private WritableContainer<Tuple> exBuffer = null;

	private final Object lockInBuffer = new Object();
	private final Object lockExBuffer = new Object();
	
	public static final int N_WBUFFS = 2;
	@SuppressWarnings("unchecked")
	private WritableContainer<Tuple>[] writeBuffer = 
			(WritableContainer<Tuple>[]) java.lang.reflect.Array.newInstance(
			 WritableContainer.class, N_WBUFFS);
	private int currWBuffIndex = 0;
	private boolean removeWChunkDone[] = new boolean[N_WBUFFS]; 
	private final Object removeWChunk = new Object(),
			lockWriteBuffer[] = new Object[N_WBUFFS];
	
	private int nChainsReceived = 0;
	private int nBucketReceived = 0;

	private final byte[] sequencesReceived = new byte[Consts.MAX_SEGMENTS_RECEIVED];
	private int highestSequence;
	private final Map<Long, Integer> childrens = new HashMap<Long, Integer>();
	private boolean isFinished;

	private boolean receivedMainChain;

	private int submissionNode;
	private int submissionId;
	private StatisticsCollector stats;

	boolean gettingData;

	private long elementsInCache = 0;
	Factory<WritableContainer<Tuple>> fb = null;

	CachedFilesMerger merger;

	// Used for unsorted streams.
	private final List<FDataInput> cacheFiles = new ArrayList<FDataInput>();
	Map<byte[], FileMetaData> sortedCacheFiles = new HashMap<byte[], FileMetaData>();
	SortedList<byte[]> minimumSortedList = new SortedList<byte[]>(100,
			new Comparator<byte[]>() {
				@Override
				public int compare(byte[] o1, byte[] o2) {
					return -comparator.compare(o1, 0, o1.length, o2, 0,
							o2.length);
				}
			});
	RawComparator<Tuple> comparator = null;
	private boolean isInBufferSorted = true;
	private boolean isExBufferSorted = true;

	public void init(long key, StatisticsCollector stats, int submissionNode,
			int submissionId, String comparator, byte[] params,
			Factory<WritableContainer<Tuple>> fb, CachedFilesMerger merger) {
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

		notifier = null;
		iter = null;

		elementsInCache = 0;

		sortedCacheFiles.clear();
		minimumSortedList.clear();
		cacheFiles.clear();
		childrens.clear();

		this.stats = stats;
		this.submissionNode = submissionNode;
		this.submissionId = submissionId;

		isInBufferSorted = true;
		if (comparator != null && comparator.length() > 0) {
			setSortingFunction(comparator, params);
		} else {
			this.comparator = null;
		}
	}

	public long getKey() {
		return key;
	}

	public synchronized void updateCounters(long idChain, long idParentChain,
			int children, boolean isResponsible) throws IOException {

		if (log.isDebugEnabled()) {
			log.debug("Update counters of bucket " + this.key + ": ic "
					+ idChain + " p " + idParentChain + " c " + children + " "
					+ isResponsible);
		}

		if (children > 0) { // Set the expected children in the
			// map
			Integer c = childrens.get(idChain);
			if (c == null) {
				childrens.put(idChain, children);
			} else {
				c += children;
				if (c == 0) {
					childrens.remove(idChain);
				} else {
					childrens.put(idChain, c);
				}
			}

		}

		if (isResponsible) { // It is a root chain
			receivedMainChain = true;
		} else {
			Integer c = childrens.get(idParentChain);
			if (c == null) {
				childrens.put(idParentChain, -1);
			} else {
				c--;
				if (c == 0) {
					childrens.remove(idParentChain);
				} else {
					childrens.put(idParentChain, c);
				}
			}
		}

		nChainsReceived++;

		checkFinished();
	}

	public synchronized void updateCounters(int sequence, boolean lastSequence)
			throws IOException {

		if (log.isDebugEnabled()) {
			log.debug("updateCounters of bucket: " + this.key + ", sequence = "
					+ sequence + ", lastSequence = " + lastSequence);
		}

		sequencesReceived[sequence]++;
		if (highestSequence < sequence)
			highestSequence = sequence;
		if (lastSequence) {
			nBucketReceived++;
			for (int i = 0; i < sequence + 1; ++i) {
				sequencesReceived[i]--;
			}
		}
		
		checkFinished();
	}

	private void checkFinished() throws IOException {
		
		if (nChainsReceived == nBucketReceived && highestSequence != -1
				&& childrens.size() == 0 && receivedMainChain) {
			
			for (int i = 0; i < highestSequence + 1; ++i) {
				if (sequencesReceived[i] != 0) {
					return;
				}
			}
			
			if (log.isDebugEnabled()) {
				log.debug("Calling setFinished on bucket " + this.key);
			}
			
			setFinished(true);
		}
	}

	public void addAll(WritableContainer<Tuple> newTuplesContainer, boolean isSorted,
			Factory<WritableContainer<Tuple>> factory) throws Exception {	
		// Sync with exBuffer -> cacheBuffer() will be sync'd on Bucket.thi,
		// combineInExBuffers() on both objects.
		synchronized (lockExBuffer) {
			long time = System.currentTimeMillis();

			if (exBuffer == null) {
				exBuffer = fb.get();
				exBuffer.clear();
				isExBufferSorted = true;
			}

			// LOG-DEBUG
			if (log.isDebugEnabled()) {
				log.debug("addAll: adding a sorted = " + isSorted + " buffer with " +
						newTuplesContainer.getNElements() + " elements " +  
						"TO exBuffer, sorted = " + isExBufferSorted + " with " +
						exBuffer.getNElements() + " elements");
			}

			// If factory is not null, we get control over the newTuplesContainer,
			// which means that we have to remove it
			boolean isExBufferEmpty = (exBuffer.getNElements() == 0);
			boolean response = false;

			if (isExBufferEmpty) {
				releaseExBuffer();
				exBuffer = newTuplesContainer;
				isExBufferSorted = isSorted;
				response = true;
			}
			else {
				if (newTuplesContainer.getNElements() > exBuffer.getNElements()) {
					response = newTuplesContainer.addAll(exBuffer);

					if (response) {
						releaseExBuffer();
						exBuffer = newTuplesContainer;
					}
				}
				else {
					response = exBuffer.addAll(newTuplesContainer);
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
					if (factory != null) {
						// We can just use the other container
						cacheBuffer(newTuplesContainer, isSorted, factory);
					} else {
						// Copy the container ...
						WritableContainer<Tuple> box = fb.get();
						newTuplesContainer.copyTo(box);
						cacheBuffer(box, isSorted, fb);
					}
				}
			}

			stats.addCounter(submissionNode, submissionId,
					"Bucket:addAll: overall time (ms)",
					System.currentTimeMillis() - time);
		}
	}

	private void combineInExBuffers() throws Exception {
		// Sync with inBuffer 
		synchronized (lockInBuffer) {
			// Sync with exBuffer [already sync'd with inBuffer]
			synchronized (lockExBuffer) {
				long time = System.currentTimeMillis();

				if (exBuffer == null || exBuffer.getNElements() == 0) {
					return;
				}

				if (inBuffer == null) {
					inBuffer = fb.get();
					inBuffer.clear();
					isInBufferSorted = true;
				}

				if (!isFinished()) {
					throw new Exception("combineInExBuffers: bucket is not yet finished!!");
				}

				boolean response = false; 
				isInBufferSorted = isInBufferSorted && isExBufferSorted;

				if (exBuffer.getNElements() > inBuffer.getNElements()) {
					// LOG-DEBUG
					if (log.isDebugEnabled()) {
						log.debug("combineInExBuffers: adding inBuffer, sorted = " + isInBufferSorted + " with " +
								inBuffer.getNElements() + " elements " +
								"TO exBuffer, sorted = " + isExBufferSorted + " with " +
								exBuffer.getNElements() + " elements");
					}

					response = exBuffer.addAll(inBuffer);

					if (!response) {
						// Cache inBuffer and replace it with exBuffer
						cacheBuffer(inBuffer, isInBufferSorted);
						isInBufferSorted = isExBufferSorted;
					}

					// Replace inBuffer with exBuffer
					inBuffer = exBuffer;
				}
				else {
					// LOG-DEBUG
					if (log.isDebugEnabled()) {
						log.debug("combineInExBuffers: adding exBuffer, sorted = " + isExBufferSorted + " with " +
								exBuffer.getNElements() + " elements " +
								"TO inBuffer, sorted = " + isInBufferSorted + " with " +
								inBuffer.getNElements() + " elements");
					}

					response = inBuffer.addAll(exBuffer);

					if (!response) {	
						// Cache exBuffer and reset it afterwards
						cacheBuffer(exBuffer, isExBufferSorted);
					}
				}

				// Reset exBuffer
				exBuffer = fb.get();
				exBuffer.clear();

				stats.addCounter(submissionNode, submissionId,
						"Bucket:combineInExBuffers: overall time (ms)",
						System.currentTimeMillis() - time);
			}
		}
	}
	
	public synchronized void copyTo(Bucket bucket) throws Exception {
		// Sync with inBuffer
		synchronized (lockInBuffer) {
			if (inBuffer == null) {
				inBuffer = fb.get();
				inBuffer.clear();
				isInBufferSorted = true;
			}

			waitForCachers();

			if (inBuffer.getNElements() > 0) {
				if (comparator != null && !isInBufferSorted) {
					inBuffer.sort(comparator, fb);
					isInBufferSorted = true;
				}

				bucket.addAll(inBuffer, isInBufferSorted, null);
			}

			if (elementsInCache > 0) {
				// There are some files to move in the new buffer
				synchronized (bucket) {
					if (comparator == null) {
						bucket.cacheFiles.addAll(cacheFiles);
					} 
					else {
						bucket.sortedCacheFiles.putAll(sortedCacheFiles);
						
						for (byte[] min : minimumSortedList) {
							bucket.minimumSortedList.add(min);
						}
					}
					
					bucket.elementsInCache += elementsInCache;
				}
			}
		}
	}

	public boolean add(Tuple tuple) throws Exception {
		// Sync with inBuffer 
		synchronized (lockInBuffer) {
			long time = System.currentTimeMillis();

			if (inBuffer == null) {
				inBuffer = fb.get();
				inBuffer.clear();
				isInBufferSorted = true;
			}

			boolean response = inBuffer.add(tuple);

			if (response) {
				isInBufferSorted = inBuffer.getNElements() < 2;
			} 
			else {
				cacheBuffer(inBuffer, isInBufferSorted);
				inBuffer = fb.get();
				inBuffer.clear();
				
				response = inBuffer.add(tuple);
				isInBufferSorted = true;

				if (!response) {
					throw new Exception(
							"The buffer is too small! Must increase the buffer size.");
				}
			}

			stats.addCounter(submissionNode, submissionId,
					"Bucket:add: overall time (ms)",
					System.currentTimeMillis() - time);

			return response;
		}
	}

	public synchronized void setFinished(boolean value) throws IOException {
		isFinished = value;

		// Combine internal + external buffers before finish
		try {
			combineInExBuffers();
		} 
		catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}

		if (isFinished && notifier != null) {
			notifier.markReady(iter);
			notifier = null;
			iter = null;
		}

		notifyAll();
		prepareRemoveWChunk();
	}

	public synchronized boolean isFinished() {
		return isFinished;
	}

	public synchronized boolean waitUntilFinished() {
		
		try {
			while (!isFinished) {
				if (log.isDebugEnabled()) {
					log.debug("waitUntilFinished on bucket " + this.key);
				}

				wait();

				if (log.isDebugEnabled()) {
					log.debug("waitUntilFinished on bucket " + this.key
							+ " done");
				}
			}
		} 
		catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}

		return true;
	}

	private boolean compareWithSortedList(byte[] element) {
		return minimumSortedList.size() == 0
				|| (element != null && minimumSortedList.comparator.compare(
					element, minimumSortedList.getLastElement()) >= 0);
	}

	private boolean copyFullFile(FileMetaData meta,
			WritableContainer<Tuple> tmpBuffer, byte[] minimum)
			throws Exception {
		// Check whether the last element is smaller than the second minimum.
		// If it is, then we can copy the entire file in the buffer.
		if (compareWithSortedList(meta.lastElement)) {
			// Copy the entire file in the buffer
			if (tmpBuffer.addAll(meta.stream, meta.lastElement, meta.nElements,
					meta.remainingSize)) {
				elementsInCache -= meta.nElements;
				meta.stream.close();
				sortedCacheFiles.remove(minimum);
				
				return true;
			}
		}
		
		return false;
	}

	private void readFrom(FileMetaData meta, byte[] buf) throws IOException {
		meta.stream.readFully(buf);
		meta.remainingSize -= buf.length + 4;
		meta.nElements--;
	}

	private void prepareRemoveWChunk() {
		// LOG-DEBUG
		if (log.isDebugEnabled()) {
			log.debug("prepareRemoveWChunk: init variables...");
		}
		
		currWBuffIndex = 0;
		for (int i = 0; i < N_WBUFFS; i++) {
			lockWriteBuffer[i] = new Object();
		}
				
		ThreadPool.createNew(new Runnable() {
			@Override
			public void run() {
				fillWriteBuffers();
			}
		}, "FillWriteBuffers");
	}
	
	private void fillWriteBuffers() {
		int wBuffIndex = currWBuffIndex;
		
		// LOG-DEBUG
		if (log.isDebugEnabled()) {
			log.debug("fillWriteBuffers: start the thread for double-buffering...");
		}
		
		for (;;) {
			long timeStart, timeEnd;
			
			synchronized (lockWriteBuffer[wBuffIndex]) {
				if (writeBuffer[wBuffIndex] == null) {
					writeBuffer[wBuffIndex] = fb.get();
					writeBuffer[wBuffIndex].clear();
				}
				
				while (writeBuffer[wBuffIndex].getNElements() > 0) {
					try {
						lockWriteBuffer[wBuffIndex].wait();
					} catch (InterruptedException e) {
						log.error(e.getMessage());
						e.printStackTrace();
					}
				}
				
				// LOG-DEBUG
				if (log.isDebugEnabled()) {
					log.debug("fillWriteBufers: fill writeBuffer[" + wBuffIndex + "], " +
							"call removeChunk");
				}
				
				timeStart = System.currentTimeMillis();
				removeChunk(writeBuffer[wBuffIndex]);
				timeEnd = System.currentTimeMillis();
				
				if (writeBuffer[wBuffIndex].getNElements() == 0) {
					// LOG-DEBUG
					if (log.isDebugEnabled()) {
						log.debug("fillWriteBufers: done, no more chunks to fill with, " +
								"stop the thread for double-buffering & notify all...");
					}
					
					removeWChunkDone[wBuffIndex] = true;
					lockWriteBuffer[wBuffIndex].notifyAll();
					return;
				}
				
				lockWriteBuffer[wBuffIndex].notifyAll();
				wBuffIndex = (wBuffIndex + 1) % N_WBUFFS;
			}
			
			stats.addCounter(submissionNode, submissionId,
					"Bucket:removeChunk: overall time (ms)",
					(timeStart - timeEnd));
		}
	}
	
	public void removeWChunk(WritableContainer<Tuple> tmpBuffer) {
		// Synchronize
		synchronized(removeWChunk) {
			boolean done = false;
			long timeStart = System.currentTimeMillis();
						
			synchronized (lockWriteBuffer[currWBuffIndex]) {
				if (writeBuffer[currWBuffIndex] == null) {
					writeBuffer[currWBuffIndex] = fb.get();
					writeBuffer[currWBuffIndex].clear();
				}
				
				// LOG-DEBUG
				if (log.isDebugEnabled()) {
					log.debug("removeWChunk: attempt removing write chunks from writeBuffers");
				}
				
				while (writeBuffer[currWBuffIndex].getNElements() == 0 
						&& !removeWChunkDone[currWBuffIndex]) {
					try {
						lockWriteBuffer[currWBuffIndex].wait();
					} 
					catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				try {
					done = removeWChunkDone[currWBuffIndex];
					
					if (done) {
						// LOG-DEBUG
						if (log.isDebugEnabled()) {
							log.debug("removeWChunk: done, no more chunks to remove");
						}
						
						tmpBuffer.clear();		
						
						stats.addCounter(submissionNode, submissionId,
								"Bucket:removeWChunk: overall time (ms)",
								System.currentTimeMillis() - timeStart);
						
						return;
					}
					
					tmpBuffer.addAll(writeBuffer[currWBuffIndex]);
					writeBuffer[currWBuffIndex].clear();

					// LOG-DEBUG
					if (log.isDebugEnabled()) {
						log.debug("removeWChunk: added " + tmpBuffer.getNElements() + 
								" tuples to tmpBuffer from" +
								" writeBuffer[" + currWBuffIndex  + "]");
					}

					lockWriteBuffer[currWBuffIndex].notifyAll();
					currWBuffIndex = (currWBuffIndex + 1) % N_WBUFFS;
				} 
				catch (Exception e) {
					e.printStackTrace();
				}
			}
		
			stats.addCounter(submissionNode, submissionId,
					"Bucket:removeWChunk: overall time (ms)",
					System.currentTimeMillis() - timeStart);
		}
	}
	
	public synchronized boolean removeChunk(WritableContainer<Tuple> tmpBuffer) {
		// Sync with inBuffer
		synchronized (lockInBuffer) {
			if (log.isDebugEnabled()) {
				log.debug("removeChunk: fill tmpBuffer with triples from bucket " + this.getKey());
			}

			long totTime = System.currentTimeMillis();
			gettingData = true;

			// If some threads still have to finish writing
			waitForCachers();

			try {
				if (comparator != null && !isInBufferSorted) {
					inBuffer.sort(comparator, fb);
					isInBufferSorted = true;
				}

				if (elementsInCache > 0) {
					if (comparator == null) { // No sorting applied
						long time = System.currentTimeMillis();
						FDataInput di = cacheFiles.remove(0);
						tmpBuffer.readFrom(di); // Read the oldest file
						stats.addCounter(submissionNode, submissionId,
								"Bucket:removeChunk: time reading from cache (ms)",
								System.currentTimeMillis() - time);
						stats.addCounter(submissionNode, submissionId,
								"Bucket:removeChunk: bytes read from cache", tmpBuffer.bytesToStore());
						elementsInCache -= tmpBuffer.getNElements();
						di.close();
					} 
					else { 
						// Need to sort
						if (log.isDebugEnabled()) {
							log.debug("Try add the first triple from the in-memory ds to the pool => " +
									"tuples.getNElements() = " + inBuffer.getNElements() + ", " + 
									"minmumSortedlist.size() = " + minimumSortedList.size());
						}

						// Add the first triple from the in-memory ds to the pool					
						if (inBuffer.getNElements() > 0
								&& minimumSortedList.size() == sortedCacheFiles.size()) {
							byte[] key = inBuffer.removeRaw(null);
							minimumSortedList.add(key);

							if (log.isDebugEnabled()) {
								log.debug("First triple from the in-memory ds was added to the pool.");
							}
						}
						else {
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
							// Remove the minimum of the tuples and try to add it to
							// the buffer.
							byte[] minimum = minimumSortedList.removeLastElement();
							insertResponse = tmpBuffer.addRaw(minimum);

							if (insertResponse) {
								if (sortedCacheFiles.containsKey(minimum)) {
									tuplesFromStream++;
									elementsInCache--;
									// The minimum came from a file. Check if the
									// file can be copied completely.
									FileMetaData meta = sortedCacheFiles
											.get(minimum);

									if (copyFullFile(meta, tmpBuffer, minimum)) {
										tuplesFromStream += meta.nElements;
										continue;
									}

									// No, it could not. Now try to stay with this
									// file as long as we can.
									try {
										int length;
										while ((length = meta.stream.readInt()) == minimum.length) {
											// log.debug("Read length " + length +
											// ", filename = " + meta.filename);
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
										// We get here if the length is different
										// (in which case we still have to read
										// the tuple, in a new buffer), or when the
										// order was wrong.
										if (length != minimum.length) {
											// log.debug("Read new length " + length
											// + ", filename = " + meta.filename);
											sortedCacheFiles.remove(minimum);
											if (length > 0) {
												// log.warn("The buffer is resized! New length = "
												// + length);
												byte[] rawValue = new byte[length];
												readFrom(meta, rawValue);
												minimumSortedList.add(rawValue);
												sortedCacheFiles
												.put(rawValue, meta);
											} else { // File is finished.
												meta.stream.close();
											}
										}
									} catch (Exception e) {
										log.warn("Here it should never come!");
										sortedCacheFiles.remove(minimum);
										meta.stream.close();
									}
								} else { // It came from the in-memory container.
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
				}

				if (elementsInCache == 0) {
					if (minimumSortedList.size() > 0) {
						// There cannot be more than 1 here
						byte[] minimum = minimumSortedList.remove(minimumSortedList
								.size() - 1);
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
			}

			stats.addCounter(submissionNode, submissionId,
					"Bucket:removeChunk: overall time (ms)",
					System.currentTimeMillis() - totTime);

			return isFinished && elementsInCache == 0
					&& (inBuffer == null || inBuffer.getNElements() == 0)
					&& (sortedCacheFiles == null || minimumSortedList.size() == 0);
		}
	}

	public synchronized boolean availableToTransmit() {
		return elementsInCache > 0
				|| (inBuffer != null && inBuffer.bytesToStore() > Consts.MIN_SIZE_TO_SEND);
	}

	int numCachers;
	int numWaitingForCachers;
	private ChainNotifier notifier;
	private TupleIterator iter;

	private void cacheBuffer(final WritableContainer<Tuple> buffer,
			final boolean isSorted) throws IOException {
		
		if (buffer.getNElements() > 0) {
			if (log.isDebugEnabled()) {
				log.debug("cacheBuffer: caching buffer, #elems = " + buffer.getNElements() +
						", sorted = " + isSorted);
			}

			cacheBuffer(buffer, isSorted, fb);
		}
	}

	private void checkFile(File name) throws IOException {
		FDataInput is = new FDataInput(new BufferedInputStream(
			new SnappyInputStream(
				new FileInputStream(name)), 65536));
		int length;
		
		while ((length = is.readInt()) > 0) {
			if (length > 256) {
				log.debug("OOPS: length = " + length);
			}
			byte[] rawValue = new byte[length];
			is.readFully(rawValue);
		}
		
		is.close();
	}

	private void cacheBuffer(final WritableContainer<Tuple> buffer,
			final boolean sorted, final Factory<WritableContainer<Tuple>> fb)
			throws IOException {

		if (buffer.getNElements() == 0) {
			// nothing to cache.
			return;
		}

		synchronized (Bucket.this) {
			elementsInCache += buffer.getNElements();
			numCachers++;
		}

		ThreadPool.createNew(new Runnable() {
			@Override
			public void run() {
				try {
					if (comparator != null && !sorted) {
						buffer.sort(comparator, fb);
					}

					File cacheFile = File.createTempFile("cache", "tmp");
					cacheFile.deleteOnExit();

					BufferedOutputStream fout = new BufferedOutputStream(
							new SnappyOutputStream(new FileOutputStream(
									cacheFile)), 65536);
					FDataOutput cacheOutputStream = new FDataOutput(fout);

					long time = System.currentTimeMillis();
					if (comparator == null) {
						buffer.writeTo(cacheOutputStream);
					} else {
						buffer.writeElementsTo(cacheOutputStream);
						cacheOutputStream.writeInt(0);
					}

					stats.addCounter(submissionNode, submissionId,
							"Bucket:cacheBuffer: overall time (ms)",
							System.currentTimeMillis() - time);

					cacheOutputStream.close();

					/*
					 * if (log.isDebugEnabled()) { 
					 * 		checkFile(cacheFile); 
					 * }
					 */

					// Register file in the list of cachedBuffers
					FDataInput is = new FDataInput(new BufferedInputStream(
							new SnappyInputStream(
									new FileInputStream(cacheFile)), 65536));

					synchronized (Bucket.this) {
						if (comparator == null) {
							cacheFiles.add(is);
						} else {
							// Read the first element and put it into the map
							try {
								int length = is.readInt();
								byte[] rawValue = new byte[length];
								is.readFully(rawValue);

								FileMetaData meta = new FileMetaData();
								meta.filename = cacheFile.getAbsolutePath();
								meta.stream = is;
								meta.nElements = buffer.getNElements() - 1;
								meta.lastElement = buffer.returnLastElement();
								if (log.isDebugEnabled()) {
									log.debug("Size of first element is "
											+ length
											+ ", size of last element is "
											+ meta.lastElement.length);
								}
								meta.remainingSize = buffer
										.getRawElementsSize() - 4 - length;

								sortedCacheFiles.put(rawValue, meta);
								minimumSortedList.add(rawValue);

							} catch (Exception e) {
								log.error("Error", e);
							}

							if (sortedCacheFiles.size() > 8) {
								merger.newRequest(Bucket.this);
							}
						}
					}
					// fb.release(buffer);
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

	private synchronized void waitForCachers() {
		numWaitingForCachers++;
		
		while (numCachers > 0) {
			if (log.isDebugEnabled()) {
				log.debug("Waiting for cachers: " + numCachers);
			}
			
			try {
				wait();
			} 
			catch (InterruptedException e) {
				log.error(e.getMessage());
				e.printStackTrace();
			}
		}
		
		numWaitingForCachers--;
	}

	@SuppressWarnings("unchecked")
	public void setSortingFunction(String sortingFunction, byte[] params) {
		
		try {
			this.comparator = (RawComparator<Tuple>) Class.forName(
					sortingFunction).newInstance();
			comparator.init(params);
		} 
		catch (Exception e) {
			log.error("Error instantiating the comparator.", e);
		}
	}

	public RawComparator<Tuple> getSortingFunction() {
		return comparator;
	}

	public synchronized void releaseBuffers() {
		releaseInBuffer();
		releaseExBuffer();
	}
	
	// TODO: should be part of WritableContainer I think
	private void releaseInBuffer() {
		// Sync with inBuffer
		synchronized (lockInBuffer) {
			if (inBuffer != null) {
				fb.release(inBuffer);
				inBuffer = null;
			}
		}
	}

	// TODO: should be part of WritableContainer I think
	private void releaseExBuffer() {
		// Sync with exBuffer
		synchronized (lockExBuffer) {
			if (exBuffer != null) {
				fb.release(exBuffer);
				exBuffer = null;
			}
		}
	}

	public boolean isEmpty() {
		// Sync with inBuffer
		synchronized(lockInBuffer) {
			// Sync with exBuffer
			synchronized (lockExBuffer) {
				return elementsInCache == 0 &&
						((inBuffer == null || inBuffer.getNElements() == 0) &&
						 (exBuffer == null || exBuffer.getNElements() == 0));		
			}
		}
	}

	public synchronized long inmemory_size() {
		// Sync with inBuffer
		synchronized(lockInBuffer) {
			// Sync with exBuffer
			synchronized (lockExBuffer) {
				if (inBuffer == null) {
					if (exBuffer != null) {
						return exBuffer.inmemory_size();
					}

					return 0;
				}
				else {
					if (exBuffer == null) {
						return inBuffer.inmemory_size();
					}

					return (inBuffer.inmemory_size() + exBuffer.inmemory_size());
				}
			}
		}
	}

	public synchronized void registerFinishedNotifier(ChainNotifier notifier,
			TupleIterator iter) {
		
		if (isFinished()) {
			notifier.markReady(iter);
			return;
		}
		
		this.notifier = notifier;
		this.iter = iter;
	}
}
