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

	public synchronized void readFrom(java.io.DataInput input) throws IOException {
		super.readFrom(input);
	}

	@Override
	public synchronized boolean add(K element) {
		boolean response = super.add(element);
		if (response && waiters > 0)
			this.notify();
		return response;
	}

	@Override
	public synchronized boolean addAll(WritableContainer<K> buffer) {
		boolean response = super.addAll(buffer);
		if (response && waiters > 0)
			this.notifyAll();
		return response;
	}

	@Override
	public synchronized boolean remove(K element) {
		while (super.getNElements() == 0) {
			waiters++;
			try {
				this.wait();
			} catch (InterruptedException e) {
				// ignore
			}
			waiters--;
		}
		return super.remove(element);
	}

	@Override
	public synchronized void moveTo(WritableContainer<?> tmpBuffer) {
		super.moveTo(tmpBuffer);
	}
}