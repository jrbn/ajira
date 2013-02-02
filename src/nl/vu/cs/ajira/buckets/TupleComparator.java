package nl.vu.cs.ajira.buckets;

import nl.vu.cs.ajira.storage.RawComparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleComparator extends RawComparator<SerializedTuple> {

	static final Logger log = LoggerFactory.getLogger(TupleComparator.class);

	public long timeConverting;

	public void init(byte[] signature, byte[] fields) {

		// For every field to sort, we need to get an appropriate comparator

	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return 0;
		// if (lenFields == 0) {
		// return compareBytes(b1, s1, l1, b2, s2, l2);
		// }
		//
		// try {
		// // Read the byte structure of a tuple and extract the positions of
		// // the fields.
		// // Ceriel: added test to avoid arraybound error, and added
		// // else-part.
		// if (s1 + Consts.MAX_TUPLE_SIZE <= b1.length
		// && s2 + Consts.MAX_TUPLE_SIZE <= b2.length) {
		//
		// int posIndexA = (b1[s1 + 2] & 0xFF);
		// int startingFieldsA = s1 + 1;
		//
		// int posIndexB = (b2[s2 + 2] & 0xFF);
		// int startingFieldsB = s2 + 1;
		//
		// for (int i = 0; i < lenFields; ++i) {
		// int fieldIndex = rawFields[i];
		//
		// int off = startingFieldsA + posIndexA + fieldIndex;
		// int startFieldA = startingFieldsA + b1[off];
		// int lenFieldA = startingFieldsA + b1[off + 1] - startFieldA;
		//
		// off = startingFieldsB + posIndexB + fieldIndex;
		// int startFieldB = startingFieldsB + b2[off];
		// int lenFieldB = startingFieldsB + b2[off + 1] - startFieldB;
		//
		// int response = compareBytes(b1, startFieldA, lenFieldA, b2,
		// startFieldB, lenFieldB);
		// if (response != 0) {
		// return response;
		// }
		// }
		// } else {
		// // This will not happen often, but is needed --Ceriel
		// for (int i = 0; i < lenFields; ++i) {
		// int fieldIndex = rawFields[i];
		//
		// int x = s1 + 2;
		// if (x >= b1.length) {
		// x -= b1.length;
		// }
		// int posIndexA = (b1[x] & 0xFF);
		// int startingFields = s1 + 3;
		// int off = startingFields + posIndexA + fieldIndex;
		// if (off >= b1.length) {
		// off -= b1.length;
		// }
		// int startFieldA = startingFields + b1[off];
		// if (startFieldA >= b1.length) {
		// startFieldA -= b1.length;
		// }
		// x = off + 1;
		// if (x >= b1.length) {
		// x -= b1.length;
		// }
		// int lenFieldA = startingFields + b1[x] - startFieldA;
		//
		// x = s2 + 2;
		// if (x >= b2.length) {
		// x -= b2.length;
		// }
		// int posIndexB = (b2[x] & 0xFF);
		// startingFields = s2 + 3;
		// off = startingFields + posIndexB + fieldIndex;
		// if (off >= b2.length) {
		// off -= b2.length;
		// }
		// int startFieldB = startingFields + b2[off];
		// if (startFieldB >= b2.length) {
		// startFieldB -= b2.length;
		// }
		// x = off + 1;
		// if (x >= b2.length) {
		// x -= b2.length;
		// }
		// int lenFieldB = startingFields + b2[x] - startFieldB;
		//
		// int response = compareBytes(b1, startFieldA, lenFieldA, b2,
		// startFieldB, lenFieldB);
		// if (response != 0) {
		// return response;
		// }
		// }
		// }
		//
		// } catch (Exception e) {
		// log.error("Failed comparison", e);
		// }
		//
		// return l1 - l2;
	}
}
