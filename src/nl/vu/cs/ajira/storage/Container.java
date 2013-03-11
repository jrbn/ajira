package nl.vu.cs.ajira.storage;

import nl.vu.cs.ajira.storage.containers.WritableContainer;

public interface Container<K extends Writable> {

	public boolean add(K element);
	
	public boolean addAll(WritableContainer<K> elements);

	public boolean remove(K element);

	public int getNElements();
}
