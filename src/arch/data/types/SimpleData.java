package arch.data.types;

import arch.ActionContext;
import arch.storage.Writable;

abstract public class SimpleData extends Writable {

    abstract public int getIdDatatype();

    public boolean equals(SimpleData a, ActionContext context) {
	return false;
    }

}