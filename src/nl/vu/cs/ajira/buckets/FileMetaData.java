package nl.vu.cs.ajira.buckets;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.data.types.bytearray.FDataInput;
import nl.vu.cs.ajira.data.types.bytearray.FDataOutput;
import nl.vu.cs.ajira.storage.containers.WritableContainer;

import org.iq80.snappy.SnappyInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps information about the content (tuples) cached/stored on the disk, such
 * as where it was stored (filename), the first and last element written in the
 * file, total number of elements, how much data remained in the file (remaining
 * size) and the file stream.
 */
public final class FileMetaData extends MetaData {
	static final Logger log = LoggerFactory.getLogger(FileMetaData.class);
	private String filename;
	private long size;
	private FDataInput stream;
	private boolean finished = false;

	static List<byte[]> bufferList = new ArrayList<byte[]>();

	private static int BUFFERSZ = 1024 * 1024;

	private static synchronized byte[] getTmpBuffer() {
		if (bufferList.isEmpty()) {
			return new byte[BUFFERSZ];
		}
		return bufferList.remove(bufferList.size() - 1);
	}

	private static synchronized void releaseTmpBuffer(byte[] buf) {
		bufferList.add(buf);
	}

	public FileMetaData(String filename, byte[] min, long nEl, byte[] max,
			long sz) {
		super(min, max, nEl);
		this.filename = filename;
		this.size = sz;
		// consistencyCheck();
	}

	@Override
	public String getName() {
		return filename;
	}

	@Override
	public void finished() {
		if (!finished) {
			finished = true;
			try {
				stream.close();
			} catch (Throwable e) {
				// ignore
			}
			stream = null;
			size = 0;
			nElements = 0;
			new File(filename).delete();
		}
	}

	private void consistencyCheck() {
		try {
			stream = new FDataInput(new SnappyInputStream(
					new BufferedInputStream(new FileInputStream(filename))));
			long nel = nElements - 1;
			long sz = size;
			if (nel > 0) {
				if (sz <= 0) {
					log.error("inconsistency in file " + filename,
							new Throwable());
					throw new Error("Inconsistency");
				}
			}
			if (nel == 0 && sz != 0) {
				log.error("inconsistency in file " + filename, new Throwable());
				throw new Error("Inconsistency");
			}
			while (nel > 0) {
				// Consistency check
				int length = stream.readInt();
				if (length != current.length) {
					current = new byte[length];
				}
				if (length < 0 || length > 4096) {
					log.error("inconsistency in file " + filename,
							new Throwable());
					throw new Error("Inconsistency");
				}
				stream.readFully(current);
				sz -= 4 + length;
				nel--;
			}
			if (sz != 0) {
				log.error("inconsistency in file " + filename, new Throwable());
				throw new Error("Inconsistency");
			}
			stream.close();
		} catch (Throwable e) {
			log.error("Got exception in consistency check", e);
			throw new Error("Inconsistency", e);
		}
		stream = null;
	}

	@Override
	public void openStream() throws IOException {
		if (stream == null) {
			stream = new FDataInput(new SnappyInputStream(
					new BufferedInputStream(new FileInputStream(filename))));
		}
	}

	public long getRemainingSize() {
		return size;
	}

	@Override
	public byte[] getNextElement() throws IOException {
		nElements--;
		int length = stream.readInt();
		if (length != current.length) {
			current = new byte[length];
		}
		stream.readFully(current);
		size -= length + 4;
		return current;
	}

	public long fullCopy(FDataOutput f) throws IOException {
		if (nElements == 1) {
			finished();
			return 0;
		}
		long sz = size;
		byte[] tmpBuffer = getTmpBuffer();
		do {
			int siz = tmpBuffer.length;
			if (siz > size) {
				siz = (int) size;
			}
			stream.readFully(tmpBuffer, 0, siz);
			f.write(tmpBuffer, 0, siz);
			size -= siz;
		} while (size > 0);
		releaseTmpBuffer(tmpBuffer);
		finished();
		return sz;
	}

	@Override
	public boolean fullCopy(WritableContainer<WritableTuple> tmpBuffer)
			throws Exception {
		if (nElements > 1) {
			if (!tmpBuffer.addAll(stream, maximum, nElements - 1, size)) {
				return false;
			}
		}
		finished();
		return true;
	}
}
