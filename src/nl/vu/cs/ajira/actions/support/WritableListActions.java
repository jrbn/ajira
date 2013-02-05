package nl.vu.cs.ajira.actions.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.storage.Writable;

public class WritableListActions implements Writable {

	private List<ActionConf> actions;

	public void setActions(List<ActionConf> actions) {
		this.actions = actions;
	}

	public List<ActionConf> getActions() throws IOException {
		return actions;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int nActions = input.readByte();
		actions = new ArrayList<ActionConf>();
		for (int i = 0; i < nActions; ++i) {
			String sAction = input.readUTF();
			ActionConf a = ActionFactory.getActionConf(sAction);
			a.readFrom(input);
			actions.add(a);
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		// Write the actions
		output.writeByte(actions.size());
		for (ActionConf action : actions) {
			output.writeUTF(action.getClassName());
			action.writeTo(output);
		}
	}
}
