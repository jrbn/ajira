package nl.vu.cs.ajira.data.types;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.storage.Writable;

abstract public class SimpleData extends Writable {

    abstract public int getIdDatatype();

    /**
     * 
     * @param a is a SimpleData object
     * @param context is a ActionContext object
     * @return false
     */
    public boolean equals(SimpleData a, ActionContext context) {
	return false;
    }

}