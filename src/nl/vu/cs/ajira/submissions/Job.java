package nl.vu.cs.ajira.submissions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.storage.Writable;

public class Job implements Writable {

	private boolean waitForStatistics = true;
	private boolean printIntermediateStats = false;
	private boolean printStatistics = true;
	private int assignedBucket = -1;
	private List<ActionConf> actions;

	@Override
	public void readFrom(DataInput input) throws IOException {
		waitForStatistics = input.readBoolean();
		printIntermediateStats = input.readBoolean();
		printStatistics = input.readBoolean();
		assignedBucket = input.readInt();
		actions = new ArrayList<ActionConf>();
		int n = input.readInt();
		for (int i = 0; i < n; ++i) {
			String className = input.readUTF();
			ActionConf a = ActionFactory.getActionConf(className);
			a.readFrom(input);
		}

	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeBoolean(waitForStatistics);
		output.writeBoolean(printIntermediateStats);
		output.writeBoolean(printStatistics);
		output.writeInt(assignedBucket);
		if (actions != null) {
			output.writeInt(actions.size());
			for (ActionConf a : actions) {
				output.writeUTF(a.getClassName());
				a.writeTo(output);
			}
		} else {
			output.writeInt(0);
		}
	}

	// @Override
	// public int bytesToStore() {
	// return 0;
	// }

	public void setAssignedOutputBucket(int bucket) {
		assignedBucket = bucket;
	}

	public int getAssignedOutputBucket() {
		return assignedBucket;
	}

	public void addActions(List<ActionConf> actions) {
		this.actions = actions;
	}

	public List<ActionConf> getActions() {
		return actions;
	}
}