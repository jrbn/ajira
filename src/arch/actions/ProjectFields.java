package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.SimpleData;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class ProjectFields extends Action {

	Tuple tuple = new Tuple();
	SimpleData[] outputFields;
	int[] positions;
	boolean first;

	public void setPositions(int... positions) {
		this.positions = positions;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int length = input.readByte();
		positions = new int[length];
		for (int i = 0; i < length; ++i) {
			positions[i] = input.readByte();
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeByte(positions.length);
		for (int i = 0; i < positions.length; ++i) {
			output.writeByte(positions[i]);
		}
	}

	@Override
	public int bytesToStore() {
		return 1 + positions.length;
	}

	@Override
	public void startProcess(ActionContext context, Chain chain) throws Exception {
		first = true;
		outputFields = new SimpleData[positions.length];
	}

	@Override
	public void process(Tuple inputTuple, Chain remainingChain,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess,
			WritableContainer<Tuple> output, ActionContext context)
			throws Exception {
		if (first) {
			first = false;
			// Populate the output fields
			for (int i = 0; i < positions.length; ++i) {
				SimpleData data = context.getDataProvider().get(
						inputTuple.getType(positions[i]));
				outputFields[i] = data;
			}
		}

		for (int i = 0; i < outputFields.length; ++i) {
			inputTuple.get(outputFields[i], i);
		}

		tuple.set(outputFields);
		output.add(tuple);
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> newChains,
			WritableContainer<Chain> chainsToSend) throws Exception {
		for (int i = 0; i < outputFields.length; ++i) {
			if (outputFields[i] != null)
				context.getDataProvider().release(outputFields[i]);
		}
	}
}
