package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class CreateBranch extends Action {

	// @Override
	// public boolean blockProcessing() {
	// return true;
	// }

	Chain newChain = new Chain();

	int inputLayer = Consts.DEFAULT_INPUT_LAYER_ID;

	public void setBranchInputLayer(int inputLayer) {
		this.inputLayer = inputLayer;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		inputLayer = input.readByte();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeByte(inputLayer);
	}

	@Override
	public int bytesToStore() throws IOException {
		return 1;
	}

	@Override
	public void process(ActionContext context, Chain chain,
			Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess)
			throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToSend) throws Exception {
		chain.createBranch(context, newChain);
		newChain.replaceInputTuple(null); // Force to read the input of the
											// first action
		newChain.setInputLayerId(inputLayer);
		chainsToSend.add(newChain);
	}
}
