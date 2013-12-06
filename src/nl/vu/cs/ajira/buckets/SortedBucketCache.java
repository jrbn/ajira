package nl.vu.cs.ajira.buckets;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.cs.ajira.data.types.bytearray.FDataOutput;
import nl.vu.cs.ajira.storage.containers.WritableContainer;

import org.iq80.snappy.SnappyOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SortedBucketCache {
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
		public boolean add(T e) {
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

	private Comparator<byte[]> byteArrayComparator = new Comparator<byte[]>() {
		@Override
		public int compare(byte[] o1, byte[] o2) {
			return -comparator.compare(o1, 0, o1.length, o2, 0, o2.length);
		}
	};

	/** Threshold when to start merging cache files. */
	private static final int MIN_MERGER_THRESHOLD = 8;

	/**
	 * Threshold when it seems that the standard merger threads cannot keep up,
	 * and temporary new ones are to be created.
	 */
	private static final int MAX_MERGER_THRESHOLD = 32;

	/** Maximum number of files to merge in a single merge operation. */
	private static final int MAX_FILES_TO_MERGE = 8;

	/** Minimum number of files to merge in a single merge operation. */
	private static final int MIN_FILES_TO_MERGE = 2;

	private static final int MAX_WORKERS = 4;
	/**
	 * Counts number of contributors, be it the bucket itself or a merger
	 * thread. Initially: only the bucket.
	 */
	private int workersCount = 1;
	private int sizeWhenMergersAreDone = 0;

	private static final Logger log = LoggerFactory
			.getLogger(SortedBucketCache.class);

	private long elementsInCache;
	private final Map<byte[], MetaData> sortedCacheFiles = new HashMap<byte[], MetaData>();
	private final TupleComparator comparator = new TupleComparator();

	private final SortedList<byte[]> minimumSortedList = new SortedList<byte[]>(
			MIN_MERGER_THRESHOLD, byteArrayComparator);
	private final Bucket bucket;
	private Throwable mergerException = null;
	private boolean finished;
	private CachedFilesMerger merger = null;

	public SortedBucketCache(TupleComparator comparator, Bucket bucket,
			CachedFilesMerger merger) {
		this.bucket = bucket;
		comparator.copyTo(this.comparator);
		this.merger = merger;
	}

	private SortedBucketCache(List<FileMetaData> l, TupleComparator comparator,
			Bucket bucket) {
		comparator.copyTo(this.comparator);
		this.bucket = bucket;
		for (MetaData f : l) {
			byte[] min = f.getMinimum();
			minimumSortedList.add(min);
			sortedCacheFiles.put(min, f);
			elementsInCache += f.getNElements();
		}
		if (log.isDebugEnabled()) {
			log.debug("Created merger cache for bucket " + bucket.getKey()
					+ ", #elements = " + elementsInCache
					+ ", number of files = " + l.size());
		}
	}

	/**
	 * Method that is used to cache the content of a buffer into files on disk
	 * whenever its size exceeds a maximum limit. The idea is to spill on disk
	 * the entire in-memory buffer when its size exceeds a limit. We open a
	 * temporary file for writing the content. All the information/details
	 * (filename, size, etc) about the caching operation are kept as metadata
	 * into a hash data-structure. @see FileMetaData
	 * 
	 * @param buffer
	 *            Buffer (tuples container) to be cached
	 */
	public void cacheBuffer(final WritableContainer<WritableTuple> buffer)
			throws IOException {

		File cacheFile = File.createTempFile("cache", "tmp");
		FDataOutput cacheOutputStream = createStream(cacheFile);

		int nElements = buffer.getNElements();
		byte[] min = buffer.removeRaw(null);
		byte[] max = buffer.getLastElement();
		long sz = buffer.getRawSize();

		buffer.writeElementsTo(cacheOutputStream);
		cacheOutputStream.close();

		FileMetaData f = new FileMetaData(cacheFile.getAbsolutePath(), min,
				nElements, max, sz);

		addMetaData(f, true);
	}

	private void addMetaData(MetaData f, boolean checkMerger) {
		byte[] min = f.getMinimum();
		SortedBucketCache toMerge = null;
		synchronized (this) {
			sizeWhenMergersAreDone++;
			sortedCacheFiles.put(min, f);
			minimumSortedList.add(min);
			elementsInCache += f.getNElements();
			if (log.isDebugEnabled()) {
				log.debug("Caching buffer for bucket " + bucket.getKey() + ", "
						+ f.getNElements() + " elements");
				log.debug("sizeWhenMergersAreDone = " + sizeWhenMergersAreDone
						+ ", size = " + minimumSortedList.size());
				log.debug("workersCount = " + workersCount);
			}
			if (checkMerger && sizeWhenMergersAreDone > MIN_MERGER_THRESHOLD) {
				if (sizeWhenMergersAreDone > MAX_MERGER_THRESHOLD
						&& workersCount < MAX_WORKERS) {
					toMerge = mergeRequest();
				} else {
					merger.newRequest(this);
				}
			}
		}
		if (toMerge != null) {
			FileMetaData result = null;
			Throwable ex = null;
			try {
				result = toMerge.dump();
			} catch (Throwable e) {
				ex = e;
			}
			mergeDone(result, ex);
		}
	}

	private boolean fullCopy(MetaData meta,
			WritableContainer<WritableTuple> tmpBuffer) throws Exception {
		// Note: this method assumes that minimum is already copied and not
		// skipped yet.
		long nElements = meta.getNElements() - 1;
		if (meta.fullCopy(tmpBuffer)) {
			elementsInCache -= nElements;
			return true;
		}
		return false;
	}

	private long fullCopy(FileMetaData meta, FDataOutput f) throws IOException {
		elementsInCache -= meta.getNElements() - 1;
		return meta.fullCopy(f);
	}

	public synchronized void addContainer(
			WritableContainer<WritableTuple> container) {
		// This method is called when the bucket is finished, and serves to add
		// the container that has not been dumped to file.
		workersCount--;
		if (container != null) {
			if (container.getNElements() > 0) {
				// Encapsulate the container with a MetaData, so that it behaves
				// the
				// same as a cached file.
				addMetaData(
						new ContainerMetaData(container, bucket,
								container.getNElements()), false);
			} else {
				bucket.releaseContainer(container);
			}
		}
		if (workersCount == 0) {
			notifyAll();
		}
		if (log.isDebugEnabled()) {
			log.debug("Bucket " + bucket.getKey() + " is done! WorkersCount = "
					+ workersCount);
		}
	}

	private static FDataOutput createStream(File cacheFile)
			throws FileNotFoundException, IOException {
		cacheFile.deleteOnExit();

		OutputStream fout = new SnappyOutputStream(new BufferedOutputStream(
				new FileOutputStream(cacheFile)));
		return new FDataOutput(fout);
	}

	/**
	 * Writes the complete SortedBucketCache onto a file.
	 * 
	 * @return the FileMetaData containing the result.
	 * @throws Exception
	 */
	FileMetaData dump() throws Exception {
		File cacheFile = File.createTempFile("cache", "tmp");
		FDataOutput cacheOutputStream = createStream(cacheFile);

		byte[] min = null;
		byte[] max = null;
		long sz = 0;
		long nElements = elementsInCache;
		long written = 0;

		for (MetaData f : sortedCacheFiles.values()) {
			f.openStream();
		}

		if (log.isDebugEnabled()) {
			log.debug("Merging " + sortedCacheFiles.size() + " cache files");
		}

		while (written != nElements) {
			// Remove the minimum of the tuples.
			byte[] minimum = minimumSortedList.removeLastElement();
			if (min == null) {
				// The minimum is not represented on file.
				min = minimum;
			} else {
				// Write it.
				cacheOutputStream.writeInt(minimum.length);
				cacheOutputStream.write(minimum);
				sz += minimum.length + 4;
			}
			written++;
			// Remove its entry from the sortedCacheFiles.
			MetaData meta = sortedCacheFiles.remove(minimum);
			max = minimum;
			if (meta.getNElements() == 1) {
				meta.finished();
				continue;
			}
			int len = minimumSortedList.size();
			// See if we can copy this complete MetaData.
			if (len == 0 || compareWithSortedList(meta.getMaximum())) {
				// Yes we can.
				max = meta.getMaximum();
				written += meta.getNElements() - 1;
				sz += ((FileMetaData) meta).fullCopy(cacheOutputStream);
				continue;
			}
			// If not, just stay with this MetaData as long as we can.
			minimum = meta.getNextElement();
			while (compareWithSortedList(minimum)) {
				cacheOutputStream.writeInt(minimum.length);
				cacheOutputStream.write(minimum);
				sz += minimum.length + 4;
				written++;
				minimum = meta.getNextElement();
			}
			// And then put it back.
			sortedCacheFiles.put(minimum, meta);
			minimumSortedList.add(minimum);
		}

		cacheOutputStream.close();

		// Return a new FileMetaData object representing the merged result.
		return new FileMetaData(cacheFile.getAbsolutePath(), min, nElements,
				max, sz);
	}

	private int getMergeSize() {
		int numFiles = minimumSortedList.size();
		int numFilesToMerge = MIN_FILES_TO_MERGE * numFiles
				/ MIN_MERGER_THRESHOLD;
		if (numFilesToMerge > MAX_FILES_TO_MERGE) {
			numFilesToMerge = MAX_FILES_TO_MERGE;
		}
		return numFilesToMerge;
	}

	/**
	 * When a merger is notified, at some point it will ask what to merge. This
	 * is produced in the form of a complete SortedBucketCache, which then needs
	 * to be "dumped" on a file, using the {@link #dump()} method.
	 * 
	 * @return the SortedBucketCache to be dumped, or <code>null</code>.
	 */
	public synchronized SortedBucketCache mergeRequest() {
		if (workersCount == 0
				|| minimumSortedList.size() <= MIN_MERGER_THRESHOLD) {
			return null;
		}

		if (log.isDebugEnabled()) {
			log.debug("Creating a merge assignment for bucket "
					+ bucket.getKey() + ", workersCount = " + workersCount);
		}
		int numFilesToMerge = getMergeSize();
		sizeWhenMergersAreDone -= numFilesToMerge;

		// A list of metadata, sorted on remaining size.
		SortedList<FileMetaData> fileList = new SortedList<FileMetaData>(
				MIN_MERGER_THRESHOLD, new Comparator<FileMetaData>() {
					@Override
					public int compare(FileMetaData o1, FileMetaData o2) {
						long sz1 = o1.getRemainingSize();
						long sz2 = o2.getRemainingSize();
						if (sz1 == sz2) {
							return 0;
						}
						if (sz1 < sz2) {
							return -1;
						}
						return 1;
					}
				});

		// Add all files to fileList, in order to sort them according to
		// remaining size.
		for (MetaData f : sortedCacheFiles.values()) {
			if (f instanceof FileMetaData) {
				fileList.add((FileMetaData) f);
			}
		}

		// Keep the numFilesToMerge smallest ones. These will be merged.
		int size = fileList.size();
		for (int i = 0; i < size - numFilesToMerge; i++) {
			fileList.removeLastElement();
		}

		// Now, we have MIN_FILES_TO_MERGE files left on the list.
		// Remove their info from the cache info.
		for (FileMetaData f : fileList) {
			byte[] min = f.getMinimum();

			minimumSortedList.remove(min);
			sortedCacheFiles.remove(min);
			elementsInCache -= f.getNElements();
		}
		workersCount++;
		// Return a new SortedBucketCache, which represents the set of files
		// to be merged.
		return new SortedBucketCache(fileList, comparator, bucket);
	}

	// When a merger is done merging, it will supply the result here.
	public synchronized void mergeDone(MetaData f, Throwable e) {
		if (e != null) {
			mergerException = e;
		}
		if (f != null) {
			addMetaData(f, false);
		}
		workersCount--;
		if (workersCount == 0) {
			notifyAll();
		}
		if (log.isDebugEnabled()) {
			log.debug("Done with merge assignment for bucket "
					+ bucket.getKey() + ", workersCount = " + workersCount);
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
		return minimumSortedList.comparator.compare(element,
				minimumSortedList.getLastElement()) >= 0;
	}

	public synchronized WritableContainer<WritableTuple> removeChunk(
			boolean[] result) throws Exception {
		if (!finished) {
			while (workersCount != 0) {
				try {
					wait();
				} catch (InterruptedException e) {
					// ignore
				}
			}
			finished = true;

			if (mergerException != null) {
				throw new Exception("Got merger exception", mergerException);
			}
			if (log.isDebugEnabled()) {
				log.debug("Bucket " + bucket.getKey()
						+ " is ready to return data.");
			}
			for (MetaData d : sortedCacheFiles.values()) {
				d.openStream();
			}
		}
		long totTime = System.currentTimeMillis();

		WritableContainer<WritableTuple> tmpBuffer = bucket.getContainer();

		if (log.isDebugEnabled()) {
			log.debug("removeChunk: number of streams is "
					+ sortedCacheFiles.size());
		}

		boolean insertResponse = false;

		if (elementsInCache == 0) {
			result[0] = true;
			return tmpBuffer;
		}

		do {
			// Remove the minimum of the tuples and try to add
			// it to the buffer.
			byte[] minimum = minimumSortedList.removeLastElement();
			insertResponse = tmpBuffer.addRaw(minimum);

			if (insertResponse) {
				MetaData meta = sortedCacheFiles.remove(minimum);
				elementsInCache--;
				if (meta.getNElements() == 1) {
					meta.finished();
					continue;
				}
				int len = minimumSortedList.size();
				if (len == 0 || compareWithSortedList(meta.getMaximum())) {
					// All elements of the current metadata are smaller than the
					// next ones. First try to copy it completely.
					if (fullCopy(meta, tmpBuffer)) {
						continue;
					}
					// Now try to fill the buffer with it.
					minimum = meta.getNextElement();
					while (tmpBuffer.addRaw(minimum)) {
						elementsInCache--;
						minimum = meta.getNextElement();
					}
					insertResponse = false;
					// if (meta.getNElements() > 0) {
					// This is always the case, because we could not copy it
					// completely.
					sortedCacheFiles.put(minimum, meta);
					minimumSortedList.add(minimum);
					// }
					break;
				}

				minimum = meta.getNextElement();
				// No, it could not. Now try to stay with this MetaData as long
				// as we can. Note that here, we cannot exhaust the metadata,
				// since that
				// case is caught above.
				while (compareWithSortedList(minimum)
						&& (insertResponse = tmpBuffer.addRaw(minimum))) {
					elementsInCache--;
					minimum = meta.getNextElement();
				}
				sortedCacheFiles.put(minimum, meta);
			}
			// Put it back
			minimumSortedList.add(minimum);
		} while (insertResponse && elementsInCache > 0);

		if (log.isDebugEnabled()) {
			log.debug("Tuples read from bucket " + bucket.getKey() + ": "
					+ tmpBuffer.getNElements() + ", time: "
					+ (System.currentTimeMillis() - totTime));
		}

		result[0] = elementsInCache == 0;
		return tmpBuffer;
	}

	public synchronized void finished() {
		for (MetaData meta : sortedCacheFiles.values()) {
			meta.finished();
		}
		sortedCacheFiles.clear();
		minimumSortedList.clear();
		elementsInCache = 0;

	}

	public long getKey() {
		return bucket.getKey();
	}

	// @Override
	// public void run() {
	// try {
	// SortedBucketCache s = mergeRequest();
	// if (s != null) {
	// FileMetaData result = null;
	// Throwable ex = null;
	// try {
	// result = s.dump();
	// } catch (Throwable e) {
	// ex = e;
	// }
	// mergeDone(result, ex);
	// }
	// } finally {
	// synchronized (this) {
	// numMergers--;
	// if (numMergers == 0) {
	// notifyAll();
	// }
	// }
	// }
	// }
}
