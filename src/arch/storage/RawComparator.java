package arch.storage;

public class RawComparator<K extends Writable> {

	public void init(byte[] params) {
	}

	public byte[] getSortingParams() {
		return null;
	}

	public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2,
			int s2, int l2) {

		int end1 = s1 + l1;
		int end2 = s2 + l2;

		if (end1 <= b1.length && end2 <= b2.length) {
			// this is probably the common case.
			while (s1 < end1 && s2 < end2) {
				// int a = (b1[s1] & 0xff);
				// int b = (b2[s2] & 0xff);
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

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return compareBytes(b1, s1, l1, b2, s2, l2);
	}

}
