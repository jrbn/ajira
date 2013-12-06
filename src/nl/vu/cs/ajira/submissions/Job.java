package nl.vu.cs.ajira.submissions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.storage.Writable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Job implements Writable {

	static final Logger log = LoggerFactory.getLogger(Job.class);

	private boolean waitForStatistics = true;
	private boolean printIntermediateStats = false;
	private boolean printStatistics = true;
	private ActionSequence actions;

	@Override
	public void readFrom(DataInput input) throws IOException {
		waitForStatistics = input.readBoolean();
		printIntermediateStats = input.readBoolean();
		printStatistics = input.readBoolean();
		actions = new ActionSequence();
		int n = input.readInt();
		for (int i = 0; i < n; ++i) {
			String className = input.readUTF();
			ActionConf a = ActionFactory.getActionConf(className);
			a.readFrom(input);
			try {
				actions.add(a);
			} catch (ActionNotConfiguredException e) {
				log.error(
						"This exception should never happen since the actions were already checked.",
						e);
				throw new Error("Internal error", e);
			}
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeBoolean(waitForStatistics);
		output.writeBoolean(printIntermediateStats);
		output.writeBoolean(printStatistics);
		if (actions != null) {
			output.writeInt(actions.length());
			for (ActionConf a : actions) {
				output.writeUTF(a.getClassName());
				a.writeTo(output);
			}
		} else {
			output.writeInt(0);
		}
	}

	public void setActions(ActionSequence actions) {
		this.actions = actions;
	}

	public ActionSequence getActions() {
		return actions;
	}
}
