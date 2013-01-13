package arch.net;

import java.io.Serializable;

class SerializableNull implements Serializable {
	private static final long serialVersionUID = -8013591758025641823L;
	static final public SerializableNull instance = new SerializableNull();
}