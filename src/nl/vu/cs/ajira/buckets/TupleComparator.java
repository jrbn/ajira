package nl.vu.cs.ajira.buckets;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.bytearray.ByteArray;
import nl.vu.cs.ajira.data.types.bytearray.CBDataInput;
import nl.vu.cs.ajira.storage.RawComparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  This class represents the implementation of a comparator 
 *  that is used inside the framework for sorting tuples (data
 *  with a tuple structure).
 */
public class TupleComparator extends RawComparator<TupleSerializer> {
	static final Logger log = LoggerFactory.getLogger(TupleComparator.class);

	public long timeConverting;

	private RawComparator<? extends SimpleData>[] comparators;
	private CBDataInput reader1 = new CBDataInput(new ByteArray());
	private CBDataInput reader2 = new CBDataInput(new ByteArray());
	private int length_positions;

	/**
	 * Initialization method. Takes the array of comparators used
	 * for sorting the fields.
	 *  
	 * @param comparators
	 * 			  Comparator for each field
	 */
	public void init(RawComparator<? extends SimpleData>[] comparators) {
		this.comparators = comparators;
		length_positions = comparators.length * 2;
	}

	/**
	 * Method that copies the comparator into another,
	 * given as parameter.
	 *  
	 * @param comp
	 * 			  The destination comparator
	 */
	public void copyTo(TupleComparator comp) {
		comp.comparators = comparators;
		comp.length_positions = length_positions;
	}

	/**
	 * Compares the bytes from b1 and b2 taking in consideration that
	 * they have a Tuple structure.
	 */
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			reader1.setBuffer(b1);
			reader2.setBuffer(b2);

			// Compare all the fields one by one
			int pos_length1 = s1;
			int pos_length2 = s2;

			s1 += length_positions;
			s2 += length_positions;

			for (int i = 0; i < comparators.length; ++i) {
				reader1.setCurrentPosition(pos_length1);
				int lField1 = reader1.readShort();
				reader2.setCurrentPosition(pos_length2);
				int lField2 = reader2.readShort();

				int res;
				if ((res = comparators[i].compare(b1, s1, lField1, b2, s2,
						lField2)) != 0) {
					return res;
				}

				pos_length1 += 2;
				pos_length2 += 2;
				s1 += lField1;
				s2 += lField2;
			}

			return 0;
		} catch (Exception e) {
			log.error("Failed comparison", e);
			return 0;
		}
	}
}
