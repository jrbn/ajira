package nl.vu.cs.ajira.net;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.storage.Writable;

public class TupleInfo implements Writable {

	public long bucketKey;

	public int remoteNodeId;
	public int submissionId;
	public int bucketId;

	public int sequence;
	public long expected;
	public int nrequests;
	public long ticket;

	@Override
	public void readFrom(DataInput input) throws IOException {
		submissionId = input.readInt();
		bucketId = input.readInt();
		remoteNodeId = input.readInt();
		bucketKey = input.readLong();
		sequence = input.readInt();
		expected = input.readLong();
		nrequests = input.readInt();
		ticket = input.readLong();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(submissionId);
		output.writeInt(bucketId);
		output.writeInt(remoteNodeId);
		output.writeLong(bucketKey);
		output.writeInt(sequence);
		output.writeLong(expected);
		output.writeInt(nrequests);
		output.writeLong(ticket);
	}

	// @Override
	// public int bytesToStore() {
	// return 44;
	// }

}
