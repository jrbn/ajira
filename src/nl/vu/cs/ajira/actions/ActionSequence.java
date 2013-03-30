package nl.vu.cs.ajira.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.storage.Writable;

public class ActionSequence implements Iterable<ActionConf>, Writable {

	private final ArrayList<ActionConf> list;

	public ActionSequence() {
		list = new ArrayList<ActionConf>();
	}

	public ActionSequence(List<ActionConf> actions)
			throws ActionNotConfiguredException {
		this();
		addAll(actions);
	}

	public ActionSequence(ActionConf action)
			throws ActionNotConfiguredException {
		this();
		add(action);
	}

	@Override
	public Iterator<ActionConf> iterator() {
		return list.iterator();
	}

	public void add(ActionConf action) throws ActionNotConfiguredException {
		int paramMissing = action.validateParameters();
		if (paramMissing != -1) {
			String actionName = action.getClassName();
			String paramName = action.getParamName(paramMissing);
			throw new ActionNotConfiguredException("Action " + actionName
					+ ": the required parameter " + paramName + " is not set.");
		}
		list.add(action);
	}

	public void addAll(List<ActionConf> actions)
			throws ActionNotConfiguredException {
		for (ActionConf action : actions) {
			add(action);
		}
	}

	public int length() {
		return list.size();
	}

	public ActionConf get(int index) {
		return list.get(index);
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		list.clear();
		int size = input.readInt();
		for (int i = 0; i < size; ++i) {
			String actionName = input.readUTF();
			ActionConf action = ActionFactory.getActionConf(actionName);
			action.readFrom(input);
			list.add(action);
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(length());
		for (ActionConf action : list) {
			output.writeUTF(action.getClassName());
			action.writeTo(output);
		}
	}
}
