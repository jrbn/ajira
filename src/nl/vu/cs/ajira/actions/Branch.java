package nl.vu.cs.ajira.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.storage.Writable;

public class Branch extends Action {

	public static class BranchContent implements Writable {

		private List<ActionConf> actions;

		public void setActions(List<ActionConf> actions) {
			this.actions = actions;
		}

		protected List<ActionConf> getActions() throws IOException {
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

		@Override
		public int bytesToStore() throws IOException {
			throw new IOException("Not supported");
		}

	}

	/***** PARAMETERS *****/
	public static final int BRANCH = 0;
	public static final String S_BRANCH = "branch";

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(BRANCH, S_BRANCH, null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		if (context.isPrincipalBranch()) {
			BranchContent branch = new BranchContent();
			getParamWritable(branch, BRANCH);
			output.branch(branch.getActions());
		}
	}
}
