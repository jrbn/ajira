package nl.vu.cs.ajira.data.types;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.storage.Writable;

abstract public class SimpleData implements Writable {

	abstract public int getIdDatatype();

	public boolean equals(SimpleData a, ActionContext context) {
		return false;
	}

}