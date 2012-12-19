package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.data.types.bytearray.BDataInput;
import arch.data.types.bytearray.BDataOutput;
import arch.storage.Writable;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class CreateBranch extends Action {

	public static class Branch extends Writable {

		private int inputLayer = Consts.DEFAULT_INPUT_LAYER_ID;
		private Tuple inputTuple = new Tuple();

		private List<ActionConf> actions;
		private byte[] rawActions;
		private boolean parsedActions;

		public void setInputLayer(int inputLayer) {
			this.inputLayer = inputLayer;
		}

		public void setInputTuple(Tuple inputTuple) {
			this.inputTuple = inputTuple;
		}

		public void setActions(List<ActionConf> actions) {
			this.actions = actions;
		}

		protected List<ActionConf> getActions() throws IOException {
			if (!parsedActions) {
				BDataInput input = new BDataInput(rawActions);
				int nActions = input.readByte();
				actions = new ArrayList<ActionConf>();
				for (int i = 0; i < nActions; ++i) {
					String sAction = input.readUTF();
					ActionConf a = ActionFactory.getActionConf(sAction);
					a.readFrom(input);
					actions.add(a);
				}
				parsedActions = true;
			}
			return actions;
		}

		@Override
		public void readFrom(DataInput input) throws IOException {
			inputLayer = input.readByte();
			inputTuple.readFrom(input);

			int s = input.readInt();
			byte[] b = new byte[s];
			input.readFully(b);
			parsedActions = false;
		}

		@Override
		public void writeTo(DataOutput output) throws IOException {
			output.writeByte(inputLayer);
			inputTuple.writeTo(output);

			// Write the actions
			byte[] tmpBuffer = new byte[Consts.CHAIN_SIZE];
			BDataOutput o = new BDataOutput(tmpBuffer);
			o.writeByte(actions.size());
			for (ActionConf action : actions) {
				o.writeUTF(action.getClassName());
				action.writeTo(o);
			}
			output.writeInt(o.cb.end); // Size of the buffer
			output.write(tmpBuffer, 0, o.cb.end);
		}

		@Override
		public int bytesToStore() throws IOException {
			throw new IOException("Not supported");
		}

	}

	/***** PARAMETERS *****/
	public static final int BRANCH = 0;
	public static final String S_BRANCH = "branch";

	Chain newChain = new Chain();
	Branch branch;

	@Override
	public void setupActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(BRANCH, S_BRANCH, null, true);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		branch = (Branch) getParam(BRANCH);
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

		chain.createBranch(context, newChain);
		// Compile the chain using the instructions in the branch
		newChain.setInputLayerId(branch.inputLayer);
		newChain.setInputTuple(branch.inputTuple);
		newChain.addActions(branch.getActions());

		chainsToSend.add(newChain);
	}
}
