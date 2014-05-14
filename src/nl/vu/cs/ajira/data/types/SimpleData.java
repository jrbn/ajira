package nl.vu.cs.ajira.data.types;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.storage.Writable;

/**
 * 
 * This class provides a structure for all the primitive types.
 * 
 */
abstract public class SimpleData implements Writable {

	abstract public int getIdDatatype();

	abstract public void copyTo(SimpleData el);

	abstract public int compareTo(SimpleData el);

	abstract public boolean equals(SimpleData el, ActionContext context);

}