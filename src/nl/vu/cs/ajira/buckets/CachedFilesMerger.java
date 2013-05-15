package nl.vu.cs.ajira.buckets;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.buckets.Bucket.FileMetaData;
import nl.vu.cs.ajira.data.types.bytearray.FDataInput;
import nl.vu.cs.ajira.data.types.bytearray.FDataOutput;

import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the implementation of the cache merger. When the number
 * of cached files becomes greater than a fixed limit (now: 8) we start a
 * background thread that takes 2 by 2 streams (files), opens them and merges
 * their content, which is already sorted. It stops when the number of cached
 * files is below the fixed limit again.
 */
public class CachedFilesMerger implements Runnable {
	static final Logger log = LoggerFactory.getLogger(CachedFilesMerger.class);
	List<Bucket> requests = new ArrayList<Bucket>();
	public int threads = 0;
	public int activeThreads = 0;

	// private final Random random = new Random();

	/**
	 * This method is used to add/register a new request to merge cached files
	 * from a bucket.
	 * 
	 * @param bucket
	 *            Bucket which cached files' number is above the limit
	 */
	public synchronized void newRequest(Bucket bucket) {
		requests.add(bucket);
		this.notify();
	}

	/**
	 * Sets the number of threads to deal with all the requests handled to the
	 * merger.
	 * 
	 * @param threads
	 *            Number of threads
	 */
	public void setNumberThreads(int threads) {
		this.threads = threads;
		this.activeThreads = threads;
	}

	/**
	 * Thread run() method. Starts threads (up to the maximum set number of
	 * threads) that will process the requests to merge cached files.
	 */
	@Override
	public void run() {

		while (true) {
			Bucket bucket = null;

			synchronized (this) {
				// Wait until there is a new request
				while (requests.size() == 0) {
					activeThreads--;

					try {
						this.wait();
					} catch (InterruptedException e) {
						// Ignore
					}

					activeThreads++;
				}

				bucket = requests.remove(0);

				if (bucket.gettingData) {
					continue;
				}
			}

			boolean merge = false;
			// Metadata declaration, not data I/O streams
			FileMetaData stream1 = null;
			FileMetaData stream2 = null;
			byte[] min1 = null;
			byte[] min2 = null;

			// Check if bucket is eligible for merging
			synchronized (bucket) {
				if (bucket.sortedCacheFiles.size() > 3) {
					// Take two streams with the smallest remaining size.
					long sz = Long.MAX_VALUE;
					int listsz = bucket.minimumSortedList.size();
					int index1 = -1, index2 = -1;

					for (int i = 0; i < listsz; i++) {
						byte[] b = bucket.minimumSortedList.get(i);
						FileMetaData f = bucket.sortedCacheFiles.get(b);
						if (f == null) {
							continue;
						}
						if (f.remainingSize < sz) {
							min1 = b;
							sz = f.remainingSize;
							index1 = i;
						}
					}

					bucket.minimumSortedList.remove(index1);
					stream1 = bucket.sortedCacheFiles.remove(min1);

					sz = Long.MAX_VALUE;
					listsz = bucket.minimumSortedList.size();

					for (int i = 0; i < listsz; i++) {
						byte[] b = bucket.minimumSortedList.get(i);
						FileMetaData f = bucket.sortedCacheFiles.get(b);
						if (f == null) {
							continue;
						}
						if (f.remainingSize < sz) {
							sz = f.remainingSize;
							index2 = i;
							min2 = b;
						}
					}

					bucket.minimumSortedList.remove(index2);
					stream2 = bucket.sortedCacheFiles.remove(min2);

					bucket.numCachers++;
					merge = true;

					if (log.isDebugEnabled()) {
						log.debug("To merge: stream1.remainingSize = "
								+ stream1.remainingSize
								+ ", stream1.nElements = " + stream1.nElements
								+ ", stream2.remainingSize = "
								+ stream2.remainingSize
								+ ", stream2.nElements = " + stream2.nElements);
					}
				}
			}

			if (merge) {
				TupleComparator comp = new TupleComparator();
				bucket.getComparator().copyTo(comp);

				File cacheFile = null;
				FDataOutput cacheOutputStream = null;
				byte[] lastElement = null;
				long totalSize = 0;
				long totalelements = 0;
				byte[] tmpbuffer = new byte[1024 * 1024];

				try {
					// Compute the new number of elements (total merged
					// elements)
					totalelements = stream1.nElements + stream2.nElements + 2;
					// Create new temporary file to write new the merged
					// content
					cacheFile = File.createTempFile("merged_files", "tmp");
					cacheFile.deleteOnExit();

					OutputStream fout = new SnappyOutputStream(
							new BufferedOutputStream(new FileOutputStream(
									cacheFile), 65536));
					// Open an output stream to write into the temporary file
					cacheOutputStream = new FDataOutput(fout);

					if (stream2.lastElement != null
							&& comp.compare(stream2.lastElement, 0,
									stream2.lastElement.length, min1, 0,
									min1.length) < 0) {
						totalSize += finishStream(stream2, min2,
								cacheOutputStream, tmpbuffer);
						totalSize += finishStream(stream1, min1,
								cacheOutputStream, tmpbuffer);
						lastElement = stream1.lastElement;
						stream1.finished();
						stream2.finished();
						stream1 = stream2 = null;
					} else if (stream1.lastElement != null
							&& comp.compare(stream1.lastElement, 0,
									stream1.lastElement.length, min2, 0,
									min2.length) < 0) {
						totalSize += finishStream(stream1, min1,
								cacheOutputStream, tmpbuffer);
						totalSize += finishStream(stream2, min2,
								cacheOutputStream, tmpbuffer);
						lastElement = stream2.lastElement;
						stream1.finished();
						stream2.finished();
						stream1 = stream2 = null;
					}

					while (stream1 != null && stream2 != null) {
						if (comp.compare(min1, 0, min1.length, min2, 0,
								min2.length) < 0) {
							cacheOutputStream.writeInt(min1.length);
							cacheOutputStream.write(min1);
							totalSize += 4 + min1.length;
							lastElement = min1;

							// Fetch new minimum from stream1
							if (stream1.remainingSize > 0) {
								int length = stream1.stream.readInt();

								if (length != min1.length) {
									min1 = new byte[length];
								}

								stream1.stream.readFully(min1);
								stream1.remainingSize -= 4 + length;
							} else {
								stream1.finished();
								stream1 = null;
							}
						} else {
							cacheOutputStream.writeInt(min2.length);
							cacheOutputStream.write(min2);
							totalSize += 4 + min2.length;
							lastElement = min2;

							// Fetch new minimum from stream2
							if (stream2.remainingSize > 0) {
								int length = stream2.stream.readInt();

								if (length != min2.length) {
									min2 = new byte[length];
								}

								stream2.stream.readFully(min2);
								stream2.remainingSize -= 4 + length;
							} else {
								stream2.finished();
								stream2 = null;
							}
						}
					}

					if (stream1 != null) {
						totalSize += finishStream(stream1, min1,
								cacheOutputStream, tmpbuffer);
						lastElement = stream1.lastElement;
						stream1.finished();
					}

					if (stream2 != null) {
						totalSize += finishStream(stream2, min2,
								cacheOutputStream, tmpbuffer);
						lastElement = stream2.lastElement;
						stream2.finished();
					}

					// Close the output stream for the temporary
					// merged file
					cacheOutputStream.close();

					// Open an input stream for the merged cached file.
					// This new stream (file descriptor) will replace the
					// older ones that were merged.
					FDataInput is = new FDataInput(new SnappyInputStream(
							new BufferedInputStream(new FileInputStream(
									cacheFile), 65536)));
					int length = is.readInt();
					byte[] rawValue = new byte[length];
					is.readFully(rawValue);

					// Metada for the new cached file -- the merged one
					FileMetaData meta = new FileMetaData();
					meta.filename = cacheFile.getAbsolutePath();
					meta.stream = is;
					meta.nElements = totalelements - 1;
					meta.lastElement = lastElement;
					meta.remainingSize = totalSize - 4 - length;

					synchronized (bucket) {
						bucket.sortedCacheFiles.put(rawValue, meta);
						bucket.minimumSortedList.add(rawValue);
						bucket.numCachers--;

						// Ceriel: added notify call.
						if (bucket.numCachers == 0
								&& bucket.numWaitingForCachers > 0) {
							bucket.notifyAll();
						}
					}

					log.debug("Finished merging two segments. Sum="
							+ meta.nElements + ", remainingSize = "
							+ meta.remainingSize);
				} catch (Exception e) {
					log.error("Error in merging the two segments", e);
				}
			}
		}
	}

	// /**
	// * Gives a random integer number.
	// *
	// * @param size
	// * Maximum size for the generated random number
	// * @return The generated random integer number
	// */
	// private synchronized int nextInt(int size) {
	// return random.nextInt(size);
	// }

	/**
	 * Reads a chunk from an input stream, puts it into a temporary buffer and
	 * then writes this buffer into an output stream (basically it transfers a
	 * chunk from a file to another using an auxiliary buffer for that).
	 * 
	 * @see #run() for usage
	 * 
	 * @param stream
	 *            Metadata for the source new cached file -- includes the input
	 *            stream (source)
	 * @param min
	 *            Minimum tuple from the file
	 * @param cacheOutputStream
	 *            Output stream (destination)
	 * @param tmpbuffer
	 *            Temporary buffer used for moving the content from one stream
	 *            to another
	 * @return How many bytes were written
	 * @throws IOException
	 */
	private long finishStream(FileMetaData stream, byte[] min,
			FDataOutput cacheOutputStream, byte[] tmpbuffer) throws IOException {
		long remSize = stream.remainingSize;
		long written = min.length + 4 + remSize;
		// Fix: forgot to write min, so lost one element here!
		// --Ceriel
		cacheOutputStream.writeInt(min.length);
		cacheOutputStream.write(min);

		do {
			int size = (int) Math.min(remSize, tmpbuffer.length);
			stream.stream.readFully(tmpbuffer, 0, size);
			cacheOutputStream.write(tmpbuffer, 0, size);
			remSize -= size;
		} while (remSize > 0);

		return written;
	}
}
