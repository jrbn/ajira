package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class ReadFromBucket extends Action {

	int node;
	int bucketId;
	boolean forward = false;

	public void setBucket(int bucketId) {
		this.bucketId = bucketId;
	}

	public void setDestination(int nodeId) {
		this.node = nodeId;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		node = input.readInt();
		bucketId = input.readInt();
		forward = input.readBoolean();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(node);
		output.writeInt(bucketId);
		output.writeBoolean(forward);
	}

	@Override
	public int bytesToStore() throws IOException {
		return 9;
	}

	@Override
	public void process(ActionContext context, Chain chain,
			Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess)
			throws Exception {
		if (forward)
			output.add(inputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToSend) throws Exception {
		// Generate a new chain and send it.
		Chain newChain = new Chain();
		chain.createBranch(context, newChain);

		newChain.setInputLayerId(Consts.BUCKET_INPUT_LAYER_ID);
		newChain.replaceInputTuple(new Tuple(new TInt(newChain
				.getSubmissionId()), new TInt(bucketId), new TInt(node)));

		chainsToSend.add(newChain);
	}

	public void setForwardTuples(boolean b) {
		forward = b;
	}

}
