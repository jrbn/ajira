package nl.vu.cs.ajira.storage;

import nl.vu.cs.ajira.storage.containers.WritableContainer;

public interface Container<K extends Writable> {

	public boolean add(K element) throws Exception;
	public boolean addAll(WritableContainer<K> elements) throws Exception;

	public boolean get(K element, int index) throws Exception;
	public boolean remove(K element) throws Exception;

	public int getNElements();
}
