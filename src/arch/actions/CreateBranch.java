package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.Writable;
import arch.storage.container.WritableContainer;

public class CreateBranch extends Action {

	public static class Branch extends Writable {

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
	public void setupActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(BRANCH, S_BRANCH, null, true);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToSend) throws Exception {

		Chain newChain = new Chain();
		Branch branch = new Branch();
		getParamWritable(branch, BRANCH);

		chain.branch(newChain);
		// Compile the chain using the instructions in the branch
		newChain.addActions(branch.getActions());

		chainsToSend.add(newChain);
	}
}
