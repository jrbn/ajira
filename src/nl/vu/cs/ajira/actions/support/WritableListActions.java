package nl.vu.cs.ajira.actions.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.storage.Writable;

/**
 * This class represents a list of {@link ActionConf} objects, in a {@link Writable} form.
 */
public class WritableListActions implements Writable {

	/** The list, initially empty. */
	private ActionConf[] actions = new ActionConf[0];

	/**
	 * Constructs a <code>WritableListActions</code> object containing an empty list.
	 */
	public WritableListActions() {
	}

	/**
	 * Constructs a <code>WritableListActions</code> object containing the specified list
	 * of {@link ActionConf} objects.
	 * @param listActions the list of {@link ActionConf} objects.
	 */
	public WritableListActions(List<ActionConf> listActions) {
		setActions(listActions);
	}

	/**
	 * Sets the list of {@link ActionConf} objects to the specified list.
	 * @param actions the list.
	 */
	public void setActions(List<ActionConf> actions) {
		this.actions = actions.toArray(new ActionConf[actions.size()]);
	}

	/**
	 * Obtains the list of {@link ActionConf} objects as an array.
	 * @return an array of {@link ActionConf} objects.
	 */
	public ActionConf[] getActions() {
		return actions;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int nActions = input.readByte();
		actions = new ActionConf[nActions];
		for (int i = 0; i < nActions; ++i) {
			String sAction = input.readUTF();
			ActionConf a = ActionFactory.getActionConf(sAction);
			a.readFrom(input);
			actions[i] = a;
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeByte(actions.length);
		for (ActionConf action : actions) {
			output.writeUTF(action.getClassName());
			action.writeTo(output);
		}
	}
}
