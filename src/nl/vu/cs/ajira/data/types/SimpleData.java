package nl.vu.cs.ajira.data.types;

import nl.vu.cs.ajira.storage.Writable;

abstract public class SimpleData implements Writable {

	abstract public int getIdDatatype();

	abstract public void copyTo(SimpleData el);

	abstract public int compareTo(SimpleData el);

}