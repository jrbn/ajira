package nl.vu.cs.ajira.buckets;

import java.io.IOException;

import nl.vu.cs.ajira.storage.containers.WritableContainer;

public abstract class MetaData {
	byte[] minimum;
	byte[] maximum;
	byte[] current;
	long nElements;

	public MetaData(byte[] min, byte[] max, long nElements) {
		this.minimum = min;
		this.maximum = max;
		this.current = new byte[1];
		this.nElements = nElements;
	}

	public boolean isFinished() {
		return nElements == 0;
	}

	public abstract byte[] getNextElement() throws IOException;

	public abstract String getName();

	public abstract boolean fullCopy(WritableContainer<WritableTuple> tmpBuffer)
			throws Exception;

	public final byte[] getMinimum() {
		return minimum;
	}

	public final byte[] getMaximum() {
		return maximum;
	}

	public final long getNElements() {
		return nElements;
	}

	public void finished() {
		// empty
	}

	public void openStream() throws IOException {
		// empty
	}
}
