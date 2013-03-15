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
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.buckets.WritableTuple;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.data.types.bytearray.BDataOutput;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.storage.Writable;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.ajira.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Chain implements Writable, InputQuery {

	static final Logger log = LoggerFactory.getLogger(Chain.class);

	private class FlowController implements ActionController {

		public boolean doNotAddAction;
		public boolean stopProcessing;
		public int destination;
		public int bucketId;
		public List<ActionConf> listActions = new ArrayList<ActionConf>();

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
	private final Tuple tuple = TupleFactory.newTuple();

	private final BDataOutput cos = new BDataOutput(buffer);
	private final BDataInput cis = new BDataInput(buffer);

	@Override
	public void readFrom(DataInput input) throws IOException {
		bufferSize = input.readInt();
		input.readFully(buffer, 0, bufferSize);

		if (input.readBoolean()) {
			// Read the number of elements and their types
			int n = input.readByte();
			SimpleData[] signature = new SimpleData[n];
			for (int i = 0; i < n; ++i) {
				signature[i] = DataProvider.getInstance().get(input.readByte());
			}
			tuple.set(signature);
			new WritableTuple(tuple).readFrom(input);
		} else {
			tuple.clear();
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(bufferSize);
		output.write(buffer, 0, bufferSize);

		if (tuple.getNElements() > 0) {
			output.writeBoolean(true);
			output.write(tuple.getNElements());
			for (int i = 0; i < tuple.getNElements(); ++i) {
				output.write(tuple.get(i).getIdDatatype());
			}
			new WritableTuple(tuple).writeTo(output);
		} else {
			output.writeBoolean(false);
		}
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

	public int getTotalChainChildren() {
		return Utils.decodeInt(buffer, 24);
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
	public void setQuery(Query query) {
		query.getTuple().copyTo(this.tuple);
	}

	@Override
	public void getQuery(Query query) {
		tuple.copyTo(query.getTuple());
	}

	public int setActions(List<ActionConf> actions, ActionContext context)
			throws Exception {

		int retval = -1;

		if (actions != null) {

			for (int i = 0; i < actions.size(); ++i) {
				ActionConf action = actions.get(i);
				int paramMissing = action.validateParameters();
				if (paramMissing != -1) {
					String actionName = action.getClassName();
					String paramName = action.getParamName(paramMissing);
					throw new Exception("Action " + actionName + " (" + i
							+ "): the required parameter " + paramName
							+ " is not set.");
				}
			}

			for (int i = actions.size() - 1; i >= 0; i--) {
				ActionConf c = actions.get(i);
				try {
					int v = addAction(c, context);
					if (i == actions.size() - 1) {
						retval = v;
					}
				} catch (Exception e) {
					log.error("Error in adding action " + c, e);
					throw new Exception("The setup of the action " + c);
				}
			}
		} else {
			throw new Exception("No action is defined");
		}
		return retval;
	}

	public void setAction(ActionConf action, ActionContext context)
			throws Exception {
		int paramMissing = action.validateParameters();
		if (paramMissing != -1) {
			String actionName = action.getClassName();
			String paramName = action.getParamName(paramMissing);
			throw new Exception("Action " + actionName
					+ ": the required parameter " + paramName + " is not set.");
		}
		addAction(action, context);
	}

	private int addAction(ActionConf action, ActionContext context)
			throws Exception {

		int retval = -1;

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
					controller.listActions = new ArrayList<ActionConf>();
					for (int i = list.size() - 1; i >= 0; --i) {
						setAction(list.get(i), context);
					}
				}
				return retval;
			}

			if (controller.stopProcessing) {
				cos.setCurrentPosition(bufferSize);
				cos.writeInt(controller.bucketId);
				cos.writeInt(controller.destination);
				cos.writeBoolean(true);
				bufferSize += 9;
				retval = controller.bucketId;
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
		int sizeAction = cos.cb.getEnd() - totalSize;
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
			controller.listActions = new ArrayList<ActionConf>();
			for (int i = list.size() - 1; i >= 0; --i) {
				setAction(list.get(i), context);
			}
		}
		return retval;
	}

	void setRawSize(int size) {
		bufferSize = size;
	}

	void copyTo(Chain newChain) {
		newChain.bufferSize = bufferSize;
		System.arraycopy(buffer, 0, newChain.buffer, 0, bufferSize);
		tuple.copyTo(newChain.tuple);
	}

	public void branch(Chain newChain, long newChainId) {
		copyTo(newChain);
		// Set up the new chain
		newChain.setParentChainId(this.getChainId());
		newChain.setChainId(newChainId);
		newChain.setTotalChainChildren(0);

		// Update counters of the new chain
		setTotalChainChildren(getTotalChainChildren() + 1);
	}

	void customBranch(Chain newChain, long parentChainId, long newChainId,
			int startFromAction) {
		int sizeToCopy = Consts.CHAIN_RESERVED_SPACE;
		if (startFromAction != -1) {
			sizeToCopy = bufferSize;
			// Remove the first n actions
			while (startFromAction-- > 0
					&& sizeToCopy > Consts.CHAIN_RESERVED_SPACE) {
				sizeToCopy -= 4;
				int sizeNameAction = Utils.decodeInt(buffer, sizeToCopy);
				sizeToCopy -= 12 + sizeNameAction; // Skip also the chainID
				sizeToCopy -= Utils.decodeInt(buffer, sizeToCopy);
				if (buffer[--sizeToCopy] == 1) {
					sizeToCopy -= 8;
				}
			}
		}

		newChain.bufferSize = sizeToCopy;
		System.arraycopy(buffer, 0, newChain.buffer, 0, sizeToCopy);

		// Set up the new chain
		newChain.setParentChainId(parentChainId);
		newChain.setChainId(newChainId);
		newChain.setTotalChainChildren(0);
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

			actions.addAction(action, chainId == currentChainId, tmpSize,
					chainId);
		}

		if (stopProcessing) {
			actions.moveComputation(nodeId, bucketId);
		}
	}
}
