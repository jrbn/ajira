package nl.vu.cs.ajira.net;

import java.io.Serializable;

/**
 * The Null class for the serializable format.
 */
class SerializableNull implements Serializable {
	private static final long serialVersionUID = -8013591758025641823L;
	static final public SerializableNull instance = new SerializableNull();
}