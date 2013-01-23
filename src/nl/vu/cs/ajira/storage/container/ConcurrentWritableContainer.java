package nl.vu.cs.ajira.storage.container;

import java.io.IOException;

import nl.vu.cs.ajira.storage.Writable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConcurrentWritableContainer<K extends Writable> extends
		WritableContainer<K> {

	static final Logger log = LoggerFactory
			.getLogger(ConcurrentWritableContainer.class);

	private int waiters = 0;

	public ConcurrentWritableContainer(int size) {
		super(size);
	}

	@Override
	public synchronized void writeTo(java.io.DataOutput output)
			throws IOException {
		super.writeTo(output);
	}

	/*
	 * @Override public synchronized void writeContentTo(java.io.DataOutput
	 * output) throws IOException { super.writeContentTo(output); }
	 */

	public synchronized void readTo(java.io.DataInput input) throws IOException {
		super.readFrom(input);
	}

	@Override
	public synchronized boolean add(K element) throws Exception {
		boolean response = super.add(element);
		if (response && waiters > 0)
			this.notify();
		return response;
	}

	@Override
	public synchronized boolean addAll(WritableContainer<K> buffer)
			throws Exception {
		boolean response = super.addAll(buffer);
		if (response && waiters > 0)
			this.notifyAll();
		return response;
	}

	@Override
	public synchronized boolean remove(K element) throws Exception {
		while (super.getNElements() == 0) {
			waiters++;
			this.wait();
			waiters--;
		}
		return super.remove(element);
	}

	@Override
	public synchronized void moveTo(WritableContainer<?> tmpBuffer) {
		super.moveTo(tmpBuffer);
	}
}