package nl.vu.cs.ajira.data.types;

import java.util.Arrays;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a generalized type of data. It can be used as a
 * collection of different types of elements that extend SimpleData.
 * 
 * @author Jacopo Urbani
 * 
 */
public class Tuple {

	protected static final Logger log = LoggerFactory.getLogger(Tuple.class);

	protected SimpleData[] signature;
	protected int nElements = 0;

	/**
	 * Constructs a empty Tuple.
	 */
	protected Tuple() {
	}

	/**
	 * Constructs a new Tuple and adds the elements from the parameter to the
	 * Tuple
	 * 
	 * @param data
	 *            is a array of SimpleData objects
	 */
	protected Tuple(SimpleData[] data) {
		set(data);
	}

	/**
	 * 
	 * @return the number of elements that are in the signature
	 */
	public int getNElements() {
		return nElements;
	}

	/**
	 * 
	 * @param pos
	 *            is the position of the element that is wanted
	 * @return the element that is at the position pos in the signature array
	 */
	public SimpleData get(int pos) {
		return signature[pos];
	}

	/**
	 * Sets the fields of the class.
	 * 
	 * @param elements
	 *            is a array or a sequence of SimpleData objects
	 */
	public void set(SimpleData... elements) {
		if (elements != null) {
			/*
			 * if (log.isDebugEnabled() && signature != null && elements.length
			 * != signature.length) { log.debug("Changing elements of tuple to "
			 * + elements.length, new Throwable()); }
			 */
			if (elements.length > Consts.MAX_TUPLE_ELEMENTS) {
				throw new Error("Too many elements in tuple");
			}
			signature = elements;
			nElements = elements.length;
		} else {
			nElements = 0;
		}
	}

	/**
	 * Sets the element found at the position pos with the parameter el.
	 * 
	 * @param el
	 *            is the element that is set at the position pos
	 * @param pos
	 *            is the position of the signature array that will be changed
	 */
	public void set(SimpleData el, int pos) {
		if (el == null) {
			return;
		}

		if (signature[pos] == null) {
			signature[pos] = DataProvider.get().get(el.getIdDatatype());
		} else if (signature[pos].getIdDatatype() != el.getIdDatatype()) {
			DataProvider.get().release(signature[pos]);
			signature[pos] = DataProvider.get().get(el.getIdDatatype());
		}

		el.copyTo(signature[pos]);
	}

	/**
	 * Copies the fields of the current Tuple in the parameter.
	 * 
	 * @param tuple
	 *            will have the same informations as the current Tuple
	 */
	public void copyTo(Tuple tuple) {
		tuple.nElements = nElements;
		if (tuple.signature == null || tuple.signature.length != nElements) {
			/*
			 * if (log.isDebugEnabled() && tuple.signature != null) {
			 * log.debug("Changing elements of tuple to " + nElements, new
			 * Throwable()); }
			 */
			tuple.signature = new SimpleData[nElements];
		}
		for (int i = 0; i < nElements; ++i) {
			tuple.set(signature[i], i);
		}
	}

	/**
	 * Compares two Tuples.
	 * 
	 * @param tuple
	 *            is the Tuple to whom the current Tuple is compared
	 * @return 0 in case of equality a number lower than if the current object
	 *         is lower than tuple a number greater than 0 if the current object
	 *         is greater than tuple
	 */
        public int compareTo(Tuple tuple) {
            if (nElements != tuple.nElements) {
                return tuple.nElements - nElements;
            }
            for (int i = 0; i < nElements; ++i) {
                    SimpleData s1 = signature[i];
                    SimpleData s2 = tuple.signature[i];
                    if (s1 == null) {
                            if (s2 != null) {
                                    return 1;
                            }
                    } else if (s2 == null) {
                            return -1;
                    } else {
                        int d1 = s1.getIdDatatype();
                        int d2 = s2.getIdDatatype();
                        if (d1 != d2) {
                            return d2 - d1;
                        }
                        int c = s1.compareTo(s2);
                        if (c != 0) {
                            return c;
                        }
                    }
            }
            return 0;
        }

	public boolean equals(Tuple tuple, ActionContext context) {
		if (nElements == tuple.nElements) {
			for (int i = 0; i < nElements; ++i) {
				if (signature[i] == null) {
					if (tuple.signature[i] != null) {
						return false;
					}
				} else if (tuple.signature[i] == null) {
					if (signature[i] != null) {
						return false;
					}
				} else if (!signature[i].equals(tuple.signature[i], context)) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Converts the Tuple to its string representation.
	 */
	@Override
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append('<');
		for (int i = 0; i < nElements; i++) {
			if (i != 0) {
				b.append(", ");
			}
			b.append(signature[i].toString());
			b.append('(');
			b.append("" + signature[i].getIdDatatype());
			b.append(')');
		}
		b.append('>');
		return b.toString();
	}

	/**
	 * Resets the fields of the class.
	 */
	public void clear() {
		nElements = 0;
		signature = null;
	}

	@Override
	/**
	 * Returns the hash code of the array signature.
	 */
	public int hashCode() {
		if (signature.length == 1) {
			return signature[0].hashCode();
		}
		return Arrays.hashCode(signature);
	}

	public int hashCode(byte[] fields) {
		int hashcode = signature[fields[0]].hashCode();
		for (int i = 1; i < fields.length; ++i) {
			hashcode += signature[fields[i]].hashCode();
		}
		return hashcode;
	}

	public String[] getSignature() {
		String[] signature = new String[nElements];
		for (int i = 0; i < nElements; ++i) {
			signature[i] = this.signature[i].getClass().getName();
		}
		return signature;
	}
}
