package nl.vu.cs.ajira.buckets;

import nl.vu.cs.ajira.storage.containers.WritableContainer;

public final class ContainerMetaData extends MetaData {

	private WritableContainer<WritableTuple> container;
	private final Bucket bucket;

	public ContainerMetaData(WritableContainer<WritableTuple> container,
			Bucket bucket, int nElements) {
		super(container.removeRaw(null), container.getLastElement(), nElements);
		this.container = container;
		this.bucket = bucket;
	}

	@Override
	public final byte[] getNextElement() {
		nElements--;
		current = container.removeRaw(current);
		return current;
	}

	@Override
	public boolean fullCopy(WritableContainer<WritableTuple> tmpBuffer)
			throws Exception {
		if (nElements == 1 || tmpBuffer.addAll(container)) {
			nElements = 0;
			return true;
		}
		return false;
	}

	@Override
	public String getName() {
		return "Container";
	}

	@Override
	public void finished() {
		bucket.releaseContainer(container);
		container = null;
	}
}
