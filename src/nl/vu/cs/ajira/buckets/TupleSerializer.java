package nl.vu.cs.ajira.buckets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.bytearray.BDataOutput;
import nl.vu.cs.ajira.storage.Writable;

/**
 * This class serializes the tuple along with the new information regarding on
 * how it can be sorted (sort order, which fields, etc) --- if are given by
 * params into the custom constructor, otherwise it acts as a simple byte
 * serializer
 */
public class TupleSerializer implements Writable {
	private boolean shouldSort = false;
	private byte[] fieldsToSort;
	private byte[] otherFields;
	private int nFields;
	private Tuple tuple;
	private int[] lengths;

	/**
	 * Custom constructor.
	 * 
	 * @param fieldsToSort
	 *            Fields to sort on (fields implied in sorting)
	 * @param nFields
	 *            Number of fields implied in sorting
	 */
	public TupleSerializer(byte[] fieldsToSort, int nFields) {
		shouldSort = true;
		this.fieldsToSort = fieldsToSort;
		this.nFields = nFields;

		if (fieldsToSort != null) {
			lengths = new int[fieldsToSort.length];
			otherFields = new byte[nFields - fieldsToSort.length];

			int c = 0;
			for (int i = 0; i < nFields && c < otherFields.length; ++i) {
				boolean found = false;
				for (int j = 0; j < fieldsToSort.length && !found; ++j) {
					if (fieldsToSort[j] == i) {
						found = true;
					}
				}
				if (!found) {
					otherFields[c++] = (byte) i;
				}
			}
		} else {
			lengths = new int[nFields];
		}

	}

	/**
	 * Default constructor.
	 */
	public TupleSerializer() {
	}

	/**
	 * Custom constructor.
	 * 
	 * @param tuple
	 *            Initial tuple (acts as a transfer buffer)
	 */
	public TupleSerializer(Tuple tuple) {
		this.tuple = tuple;
	}

	public void setTuple(Tuple tuple) {
		this.tuple = tuple;
	}

	public Tuple getTuple() {
		return tuple;
	}

	/**
	 * Reads a tuple from an input data stream.
	 */
	@Override
	public void readFrom(DataInput input) throws IOException {

		if (shouldSort) {
			// First skip a number of bytes
			if (fieldsToSort != null) {
				input.skipBytes(2 * fieldsToSort.length);

				// for (int i = 0; i < fieldsToSort.length; ++i) {
				// tuple.get(fieldsToSort[i]).readFrom(input);
				// }
				// for (int i = 0; i < otherFields.length; ++i) {
				// tuple.get(otherFields[i]).readFrom(input);
				// }

			} else {
				input.skipBytes(2 * nFields);
			}
		}

		for (int i = 0; i < tuple.getNElements(); ++i) {
			tuple.get(i).readFrom(input);
		}
	}

	/**
	 * Writes the new serialized tuple (the one that also contains extra
	 * information about how it should be sorted) into a data output stream ---
	 * only if those information are specified upwards, otherwise it will simply
	 * write the tuple as it is.
	 */
	@Override
	public void writeTo(DataOutput output) throws IOException {
		if (shouldSort) {
			// First write the fields that need to be sorted
			BDataOutput o = (BDataOutput) output;
			int startLength = o.cb.getEnd();

			if (fieldsToSort != null) {
				o.skipBytes(2 * fieldsToSort.length);
				int currentPosition = o.cb.getEnd();

				for (int i = 0; i < fieldsToSort.length; ++i) {
					tuple.get(fieldsToSort[i]).writeTo(output);

					int e = o.cb.getEnd();
					if (e >= currentPosition) {
						lengths[i] = e - currentPosition;
					} else {
						lengths[i] = o.cb.getBuffer().length - currentPosition
								+ e;
					}
					currentPosition = e;
				}

				// Write the remaining fields
				for (int i = 0; i < otherFields.length; ++i) {
					tuple.get(otherFields[i]).writeTo(output);
				}

				// Write the lengths at the beginning
				int e = o.cb.getEnd();
				o.setCurrentPosition(startLength);
				for (int i = 0; i < fieldsToSort.length; ++i) {
					o.writeShort(lengths[i]);
				}
				o.setCurrentPosition(e);
			} else {
				o.skipBytes(2 * nFields);
				int currentPosition = o.cb.getEnd();
				for (int i = 0; i < nFields; ++i) {
					tuple.get(i).writeTo(output);

					int e = o.cb.getEnd();
					if (e >= currentPosition) {
						lengths[i] = e - currentPosition;
					} else {
						lengths[i] = o.cb.getBuffer().length - currentPosition
								+ e;
					}

					currentPosition = e;
				}

				// Write the lengths at the beginning
				int e = o.cb.getEnd();

				o.setCurrentPosition(startLength);

				for (int i = 0; i < nFields; ++i) {
					o.writeShort(lengths[i]);
				}

				o.setCurrentPosition(e);
			}
		} else {
			for (int i = 0; i < tuple.getNElements(); ++i) {
				tuple.get(i).writeTo(output);
			}
		}
	}
}
