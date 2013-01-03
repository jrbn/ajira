package arch.buckets;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import arch.buckets.Bucket.FileMetaData;
import arch.data.types.Tuple;
import arch.data.types.bytearray.FDataInput;
import arch.data.types.bytearray.FDataOutput;
import arch.storage.RawComparator;

public class CachedFilesMerger implements Runnable {

	static final Logger log = LoggerFactory.getLogger(CachedFilesMerger.class);

	List<Bucket> requests = new ArrayList<Bucket>();
	public int threads = 0;
	public int activeThreads = 0;
	private Random random = new Random();

	public synchronized void newRequest(Bucket bucket) {
		requests.add(bucket);
		this.notify();
	}

	public void setNumberThreads(int threads) {
		this.threads = threads;
		this.activeThreads = threads;
	}

	@Override
	public void run() {

		byte[] tmpbuffer = new byte[1024 * 1024];

		while (true) {
			Bucket bucket = null;
			synchronized (this) {
				// Wait until there is a new request
				while (requests.size() == 0) {
					activeThreads--;
					try {
						this.wait();
					} catch (InterruptedException e) {
					}
					activeThreads++;
				}
				bucket = requests.remove(0);
				if (bucket.gettingData) {
					continue;
				}
			}

			// Check if bucket is eligible for merging
			boolean merge = false;
			FileMetaData stream1 = null;
			FileMetaData stream2 = null;
			byte[] min1 = null;
			byte[] min2 = null;
			RawComparator<Tuple> comp = null;

			synchronized (bucket) {
				if (bucket.sortedCacheFiles.size() > 3) {
					// Take one random stream
					int index1, index2;
					do {
						index1 = nextInt(bucket.minimumSortedList.size());
						min1 = bucket.minimumSortedList.get(index1);
					} while (!bucket.sortedCacheFiles.containsKey(min1));

					bucket.minimumSortedList.remove(index1);
					stream1 = bucket.sortedCacheFiles.remove(min1);

					do {
						index2 = nextInt(bucket.minimumSortedList.size());
						min2 = bucket.minimumSortedList.get(index2);
					} while (min1 == min2
							|| !bucket.sortedCacheFiles.containsKey(min2));

					// bucket.minimumSortedList.remove(index1);
					// Fix: moved to above. Removing one may affect the index of
					// the other. --Ceriel
					bucket.minimumSortedList.remove(index2);
					// stream1 = bucket.sortedCacheFiles.remove(min1);
					stream2 = bucket.sortedCacheFiles.remove(min2);
					comp = bucket.comparator;
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

			File cacheFile = null;
			FDataOutput cacheOutputStream = null;
			byte[] lastElement = null;
			long totalSize = 0;
			long totalelements = 0;
			if (merge) {

				try {
					totalelements = stream1.nElements + stream2.nElements + 2;
					cacheFile = File.createTempFile("merged_files", "tmp");
                                        cacheFile.deleteOnExit();

					BufferedOutputStream fout = new BufferedOutputStream(
							new SnappyOutputStream(new FileOutputStream(
									cacheFile)));

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
						stream1.stream.close();
						new File(stream1.filename).delete();
						stream2.stream.close();
						new File(stream2.filename).delete();
						stream1 = stream2 = null;
					}

					else if (stream1.lastElement != null
							&& comp.compare(stream1.lastElement, 0,
									stream1.lastElement.length, min2, 0,
									min2.length) < 0) {
						totalSize += finishStream(stream1, min1,
								cacheOutputStream, tmpbuffer);
						totalSize += finishStream(stream2, min2,
								cacheOutputStream, tmpbuffer);
						lastElement = stream2.lastElement;
						stream1.stream.close();
						new File(stream1.filename).delete();
						stream2.stream.close();
						new File(stream2.filename).delete();
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
									if (log.isDebugEnabled()) {
										log.debug("Reallocated min1");
									}
									min1 = new byte[length];
								}
								stream1.stream.readFully(min1);
								stream1.remainingSize -= 4 + length;
							} else {
								stream1.stream.close();
								new File(stream1.filename).delete();
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
									if (log.isDebugEnabled()) {
										log.debug("Reallocated min2");
									}
									min2 = new byte[length];
								}
								stream2.stream.readFully(min2);
								stream2.remainingSize -= 4 + length;
							} else {
								stream2.stream.close();
								new File(stream2.filename).delete();
								stream2 = null;
							}
						}
					}

					if (stream1 != null) {
						totalSize += finishStream(stream1, min1,
								cacheOutputStream, tmpbuffer);
						lastElement = stream1.lastElement;
						stream1.stream.close();
						new File(stream1.filename).delete();
					}

					if (stream2 != null) {
						totalSize += finishStream(stream2, min2,
								cacheOutputStream, tmpbuffer);
						lastElement = stream2.lastElement;
						stream2.stream.close();
						new File(stream2.filename).delete();
					}

					cacheOutputStream.close();

					FDataInput is = new FDataInput(new BufferedInputStream(
							new SnappyInputStream(
									new FileInputStream(cacheFile)), 65536));

					int length = is.readInt();
					byte[] rawValue = new byte[length];
					is.readFully(rawValue);

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

					log.info("Finished merging two segments. Sum="
							+ meta.nElements + ", remainingSize = "
							+ meta.remainingSize);

				} catch (Exception e) {
					log.error("Error in merging the two segments", e);
				}
			}
		}
	}

	private synchronized int nextInt(int size) {
		return random.nextInt(size);
	}

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
