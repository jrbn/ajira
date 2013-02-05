package nl.vu.cs.ajira.storage;

import nl.vu.cs.ajira.data.types.SimpleData;

public class RawComparator<T> {

	@SuppressWarnings("unchecked")
	private static final RawComparator<? extends SimpleData>[] cmps = new RawComparator[256];
	private static RawComparator<? extends SimpleData> defaultComparator = new RawComparator<SimpleData>();

	/**
	 * Compares the bytes from b1 and b2.
	 * @param b1 is the first array that has to be compared
	 * @param s1 is the position from where the comparison of b1 starts
	 * @param l1 is the number of bytes from b1 that have to be compared
	 * @param b2 is the second array that has to be compared
	 * @param s2is the position from where the comparison of b2 starts
	 * @param l2 is the number of bytes from b2 that have to be compared
	 * @return 0 in case of equality 
	 * 		   a number lower than 0 in case b1 is lower than b2 
	 * 		   a number greater than 0 in case b1 is greater than b2
	 */
	public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2,
			int s2, int l2) {

		int end1 = s1 + l1;
		int end2 = s2 + l2;

		if (end1 <= b1.length && end2 <= b2.length) {
			// this is probably the common case.
			while (s1 < end1 && s2 < end2) {
				if (b1[s1] != b2[s2]) {
					return (b1[s1] & 0xff) - (b2[s2] & 0xff);
				}

				++s1;
				++s2;
			}

			return l1 - l2;
		}

		while (s1 < end1 && s2 < end2) {

			if (s1 >= b1.length) {
				s1 = 0;
				end1 %= b1.length;
			}

			if (s2 >= b2.length) {
				s2 = 0;
				end2 %= b2.length;
			}

			int a = (b1[s1] & 0xff);
			int b = (b2[s2] & 0xff);
			if (a != b) {
				return a - b;
			}

			++s1;
			++s2;
		}

		return l1 - l2;
	}

	/**
	 * Compares the bytes from b1 and b2. The computations are done in the previous method. 
	 * @param b1 is the first array that has to be compared
	 * @param s1 is the position from where the comparison of b1 starts
	 * @param l1 is the number of bytes from b1 that have to be compared
	 * @param b2 is the second array that has to be compared
	 * @param s2is the position from where the comparison of b2 starts
	 * @param l2 is the number of bytes from b2 that have to be compared
	 * @return 0 in case of equality 
	 * 		   a number lower than 0 in case b1 is lower than b2 
	 * 		   a number greater than 0 in case b1 is greater than b2
	 */
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return compareBytes(b1, s1, l1, b2, s2, l2);
	}

	public static synchronized void registerComparator(int idSimpleData,
			RawComparator<? extends SimpleData> cmp) {
		cmps[idSimpleData] = cmp;
	}

	public static RawComparator<? extends SimpleData> getComparator(int id) {
		return (cmps[id] != null) ? cmps[id] : defaultComparator;
	}
}
