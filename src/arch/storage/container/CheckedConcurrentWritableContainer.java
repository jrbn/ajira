package arch.storage.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.storage.Writable;

// A ConcurrentWritableContainer that throws an error when an addition does not fit.
public class CheckedConcurrentWritableContainer<K extends Writable> extends
	ConcurrentWritableContainer<K> {

    static final Logger log = LoggerFactory
	    .getLogger(ConcurrentWritableContainer.class);

    public CheckedConcurrentWritableContainer(int size) {
	super(size);
    }

    @Override
    public boolean add(K element) throws Exception {
	boolean response = super.add(element);
	if (!response) {
	    log.error("The container is too small for this addition!", new Throwable());
	    log.error("nElements = " + nElements);
	}
	return response;
    }

    @Override
    public boolean addAll(WritableContainer<K> buffer) throws Exception {
	boolean response = super.addAll(buffer);
	if (!response) {
	    log.error("The container is too small for this addition!", new Throwable());
	    log.error("nElements = " + nElements + ", adding " + buffer.nElements);
	}
	return response;
    }
}
