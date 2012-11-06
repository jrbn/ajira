package arch.submissions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.Writable;
import arch.utils.Consts;

public class JobDescriptor extends Writable {

	private int inputLayer = Consts.DEFAULT_INPUT_LAYER_ID;
	private final Tuple inputTuple = new Tuple();
	private final List<String> action = new ArrayList<String>();
	private final List<Tuple> tuples = new ArrayList<Tuple>();
	private String availableRules = null;
	private boolean notExecuteMainProgram = false;
	private boolean waitForStatistics = true;
	private boolean printIntermediateStats = false;
	private boolean printStatistics = true;
	private int assignedBucket = -1;
	private final Chain chain = new Chain();

	@Override
	public void readFrom(DataInput input) throws IOException {
		inputLayer = input.readByte();
		inputTuple.readFrom(input);
		int n = input.readInt();
		action.clear();
		tuples.clear();
		for (int i = 0; i < n; ++i) {
			action.add(input.readUTF());
		}
		for (int i = 0; i < n; ++i) {
			if (input.readByte() == 1) {
				Tuple tuple = new Tuple();
				tuple.readFrom(input);
				tuples.add(tuple);
			} else {
				tuples.add(null);
			}
		}
		availableRules = input.readUTF();
		notExecuteMainProgram = input.readBoolean();
		waitForStatistics = input.readBoolean();
		printIntermediateStats = input.readBoolean();
		printStatistics = input.readBoolean();
		assignedBucket = input.readInt();
		chain.readFrom(input);
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeByte(inputLayer);
		inputTuple.writeTo(output);
		output.writeInt(action.size());
		for (String a : action) {
			output.writeUTF(a);
		}
		for (Tuple tuple : tuples) {
			if (tuple != null) {
				output.writeByte(1);
				tuple.writeTo(output);
			} else {
				output.writeByte(0);
			}
		}
		output.writeUTF(availableRules);
		output.writeBoolean(notExecuteMainProgram);
		output.writeBoolean(waitForStatistics);
		output.writeBoolean(printIntermediateStats);
		output.writeBoolean(printStatistics);
		output.writeInt(assignedBucket);
		chain.writeTo(output);
	}

	@Override
	public int bytesToStore() {
		return 0;
	}

	public void setAssignedOutputBucket(int bucket) {
		assignedBucket = bucket;
	}

	public int getAssignedOutputBucket() {
		return assignedBucket;
	}

	public Chain getNewChain() {
		Chain chain = new Chain();
		chain.setParentChainId(-1);
		chain.setReplicatedFactor(1);
		chain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);
		chain.setExcludeExecution(false);
		return chain;
	}

	public void setMainChain(Chain chain) {
		chain.copyTo(this.chain);
	}

	public Chain getMainChain() {
		return chain;
	}
}