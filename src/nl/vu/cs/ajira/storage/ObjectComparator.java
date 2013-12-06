package nl.vu.cs.ajira.storage;

import java.util.Comparator;

import nl.vu.cs.ajira.data.types.bytearray.BDataInput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ObjectComparator<T extends Writable> extends
	RawComparator<T> implements Comparator<T> {

    static final Logger log = LoggerFactory.getLogger(ObjectComparator.class);

    T t1;
    T t2;

    BDataInput in = new BDataInput();

    @SuppressWarnings("unchecked")
    /**
     * Creates a new ObjectComparator with two instances.
     */
    public ObjectComparator() {
		try {
			// TODO: this cannot work! Fortunately, this class is never used ....
		    t1 = (T) t1.getClass().newInstance();
		    t2 = (T) t2.getClass().newInstance();
		} catch (Exception e) {
		}
    }

    @Override
    /**
     * Compares the objects t1 and t2 using a BDataInput object.
     */
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// Create two objects from the arrays
		try {
		    in.setCurrentPosition(b1, s1);
		    t1.readFrom(in);
		    in.setCurrentPosition(b2, s2);
		    t2.readFrom(in);
		} catch (Exception e) {
		    log.error("Error", e);
		}
		return compare(t1, t2);
    }
}