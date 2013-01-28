package nl.vu.cs.ajira.chains;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionController;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.data.types.bytearray.BDataOutput;
import nl.vu.cs.ajira.datalayer.Query;
import nl.vu.cs.ajira.storage.Writable;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.ajira.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Chain implements Writable, Query {

	static final Logger log = LoggerFactory.getLogger(Chain.class);

	private class FlowController implements ActionController {

		public boolean doNotAddAction;
		public boolean stopProcessing;
		public int destination;
		public int bucketId;
		public List<ActionConf> listActions = new ArrayList<>();

		public void init() {
			stopProcessing = doNotAddAction = false;
			listActions.clear();
		}

		@Override
		public void continueComputationOn(int destination, int bucketId) {
			stopProcessing = true;
			this.destination = destination;
			this.bucketId = bucketId;
		}

		@Override
		public void doNotAddCurrentAction() {
			doNotAddAction = true;
		}

		@Override
		public void addAction(ActionConf conf) {
			listActions.add(conf);
		}
	}

	private FlowController controller = new FlowController();

	private int bufferSize = Consts.CHAIN_RESERVED_SPACE;
	private final byte[] buffer = new byte[Consts.CHAIN_SIZE];
	private Tuple inputTuple = null;

	private final BDataOutput cos = new BDataOutput(buffer);
	private final BDataInput cis = new BDataInput(buffer);

	@Override
	public void readFrom(DataInput input) throws IOException {
		bufferSize = input.readInt();
		input.readFully(buffer, 0, bufferSize);

		if (input.readBoolean()) {
			if (inputTuple == null) {
				inputTuple = TupleFactory.newTuple();
			}
			inputTuple.readFrom(input);
		} else {
			inputTuple = null;
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(bufferSize);
		output.write(buffer, 0, bufferSize);
		if (inputTuple != null) {
			output.writeBoolean(true);
			inputTuple.writeTo(output);
		} else {
			output.writeBoolean(false);
		}
	}

	@Override
	public int bytesToStore() throws IOException {
		int size = bufferSize + 9;
		if (inputTuple != null) {
			size += inputTuple.bytesToStore();
		}
		return size;
	}

	public void setSubmissionNode(int nodeId) {
		Utils.encodeInt(buffer, 0, nodeId);
	}

	public int getSubmissionNode() {
		return Utils.decodeInt(buffer, 0);
	}

	public void setSubmissionId(int submissionId) {
		Utils.encodeInt(buffer, 4, submissionId);
	}

	public int getSubmissionId() {
		return Utils.decodeInt(buffer, 4);
	}

	void setChainId(long chainId) {
		Utils.encodeLong(buffer, 8, chainId);
	}

	public long getChainId() {
		return Utils.decodeLong(buffer, 8);
	}

	public void setParentChainId(long chainId) {
		Utils.encodeLong(buffer, 16, chainId);
	}

	public long getParentChainId() {
		return Utils.decodeLong(buffer, 16);
	}

	void setTotalChainChildren(int chainChildren) {
		Utils.encodeInt(buffer, 24, chainChildren);
	}

	void setGeneratedRootChains(int rootChains) {
		Utils.encodeInt(buffer, 28, rootChains);
	}

	public int getTotalChainChildren() {
		return Utils.decodeInt(buffer, 24);
	}

	public int getGeneratedRootChains() {
		return Utils.decodeInt(buffer, 28);
	}

	@Override
	public void setInputLayer(int id) {
		buffer[32] = (byte) id;
	}

	@Override
	public int getInputLayer() {
		return buffer[32];
	}

	void setCustomFlag(byte value) {
		buffer[34] = value;
	}

	int getCustomFlag() {
		return buffer[34];
	}

	@Override
	public void setInputTuple(Tuple tuple) {
		inputTuple = tuple;
	}

	@Override
	public void getInputTuple(Tuple tuple) {
		inputTuple.copyTo(tuple);
	}

	public void addActions(List<ActionConf> actions, ActionContext context)
			throws Exception {
		if (actions != null) {
			for (int i = actions.size() - 1; i >= 0; i--) {
				addAction(actions.get(i), context);
			}
		} else {
			throw new Exception("actions is null");
		}
	}

	void addAction(ActionConf action, ActionContext context) throws Exception {

		// Validate the action
		if (!action.validateParameters()) {
			throw new Exception("Some required parameters for the action "
					+ action.getClassName() + " are not set.");
		}

		// Process the parameters and possibly insert instructions to control
		// the flow.
		if (action.getConfigurator() != null) {
			controller.init();
			action.getConfigurator().process(this, action, controller, context);

			if (controller.doNotAddAction) {
				if (action.getConfigurator() != null
						&& controller.listActions.size() > 0) {
					// Add the actions from the last
					List<ActionConf> list = controller.listActions;
					controller.listActions = new ArrayList<>();
					for (int i = list.size() - 1; i >= 0; --i) {
						addAction(list.get(i), context);
					}
				}

				return;
			}

			if (controller.stopProcessing) {
				cos.setCurrentPosition(bufferSize);
				cos.writeInt(controller.bucketId);
				cos.writeInt(controller.destination);
				cos.writeBoolean(true);
				bufferSize += 9;
			} else {
				cos.writeBoolean(false);
				bufferSize++;
			}
		} else {
			cos.writeBoolean(false);
			bufferSize++;
		}

		// Serialize the action configuration
		int totalSize = bufferSize;
		cos.setCurrentPosition(bufferSize);
		action.writeTo(cos);
		int sizeAction = cos.cb.end - totalSize;
		bufferSize += sizeAction;
		Utils.encodeInt(buffer, bufferSize, sizeAction);
		bufferSize += 4;

		// Serialize the chain id of this chain
		Utils.encodeLong(buffer, bufferSize, getChainId());
		bufferSize += 8;

		// Serialize the class name
		byte[] sAction = action.getClassName().getBytes();
		System.arraycopy(sAction, 0, buffer, bufferSize, sAction.length);
		bufferSize += sAction.length;
		Utils.encodeInt(buffer, bufferSize, sAction.length);
		bufferSize += 4;

		if (action.getConfigurator() != null
				&& controller.listActions.size() > 0) {
			// Add the actions from the last
			List<ActionConf> list = controller.listActions;
			controller.listActions = new ArrayList<>();
			for (int i = list.size() - 1; i >= 0; --i) {
				addAction(list.get(i), context);
			}
		}
	}

	void setRawSize(int size) {
		bufferSize = size;
	}

	void copyTo(Chain newChain) {
		newChain.bufferSize = bufferSize;
		System.arraycopy(buffer, 0, newChain.buffer, 0, bufferSize);
		if (inputTuple != null) {
			if (newChain.inputTuple == null) {
				newChain.inputTuple = TupleFactory.newTuple();
			}
			inputTuple.copyTo(newChain.inputTuple);
		} else {
			newChain.inputTuple = null;
		}
	}

	public void branch(Chain newChain, long newChainId) {
		copyTo(newChain);
		// Set up the new chain
		newChain.setParentChainId(this.getChainId());
		newChain.setChainId(newChainId);
		newChain.setTotalChainChildren(0);
		newChain.setGeneratedRootChains(0);

		// Update counters of the new chain.
		setTotalChainChildren(getTotalChainChildren() + 1);
	}

	void branchFromRoot(Chain newChain, long newChainId) {
		newChain.bufferSize = Consts.CHAIN_RESERVED_SPACE;
		System.arraycopy(buffer, 0, newChain.buffer, 0, bufferSize);

		// Set up the new chain
		newChain.setParentChainId(-1);
		newChain.setChainId(newChainId);
		newChain.setTotalChainChildren(0);
		newChain.setGeneratedRootChains(0);

		// Update this counter
		setGeneratedRootChains(getGeneratedRootChains() + 1);
	}

	void getActions(ChainExecutor actions, ActionFactory ap) throws IOException {

		// Read the chain and feel the actions
		int tmpSize = bufferSize;
		long currentChainId = getChainId();
		boolean stopProcessing = false;
		int nodeId = 0;
		int bucketId = 0;

		while (tmpSize > Consts.CHAIN_RESERVED_SPACE && !stopProcessing) {
			tmpSize -= 4;
			int size = Utils.decodeInt(buffer, tmpSize);
			String sAction = new String(buffer, tmpSize - size, size);

			tmpSize -= 8 + size;
			long chainId = Utils.decodeLong(buffer, tmpSize);

			// Get size of the action
			tmpSize -= 4;
			tmpSize -= Utils.decodeInt(buffer, tmpSize);
			cis.setCurrentPosition(tmpSize);
			Action action = ap.getAction(sAction, cis);

			stopProcessing = buffer[--tmpSize] == 1;
			if (stopProcessing) {
				tmpSize -= 4;
				nodeId = Utils.decodeInt(buffer, tmpSize);
				tmpSize -= 4;
				bucketId = Utils.decodeInt(buffer, tmpSize);
			}

			actions.addAction(action, chainId == currentChainId, tmpSize);
		}

		if (stopProcessing) {
			actions.moveComputation(nodeId, bucketId);
		}
	}
}
