package nl.vu.cs.ajira.storage.containers;

import nl.vu.cs.ajira.storage.Writable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// A ConcurrentWritableContainer that throws an error when an addition does not fit.
public class CheckedConcurrentWritableContainer<K extends Writable> extends
ConcurrentWritableContainer<K> {

	static final Logger log = LoggerFactory
			.getLogger(ConcurrentWritableContainer.class);

	public CheckedConcurrentWritableContainer(int size) {
		super(size);
	}

	@Override
	public boolean add(K element) {
		boolean response = super.add(element);
		if (!response) {
			log.error("The container is too small for this addition!", new Throwable());
			log.error("nElements = " + nElements);
		}
		return response;
	}

	@Override
	public boolean addAll(WritableContainer<K> buffer) {
		boolean response = super.addAll(buffer);
		if (!response) {
			log.error("The container is too small for this addition!", new Throwable());
			log.error("nElements = " + nElements + ", adding " + buffer.nElements);
		}
		return response;
	}
}
