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
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.buckets.WritableTuple;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.data.types.bytearray.BDataOutput;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.datalayer.buckets.BucketsLayer;
import nl.vu.cs.ajira.datalayer.chainsplits.ChainSplitLayer;
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;
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
			bucketId = -1;
		}

		@Override
		public void continueComputationOn(int destination, int bucketId) {
			stopProcessing = true;
			this.destination = destination;
			this.bucketId = bucketId;
		}

		public void setOutputBucket(int bucketId) {
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

	private static final int CHAIN_RESERVED_SPACE = 39;

	private final FlowController controller = new FlowController();

	private int bufferSize = CHAIN_RESERVED_SPACE;
	private byte[] buffer = new byte[Consts.INITIAL_CHAIN_SIZE];

	private final Tuple tuple = TupleFactory.newTuple();
	private String inputLayer = null;

	private final BDataOutput cos = new BDataOutput(buffer);
	private final BDataInput cis = new BDataInput(buffer);

	@Override
	public void readFrom(DataInput input) throws IOException {
		bufferSize = input.readInt();
		grow(bufferSize, true);
		input.readFully(buffer, 0, bufferSize);

		if (buffer[32] == -1) {
			int len = input.readByte();
			byte[] bytes = new byte[len];
			input.readFully(bytes);
			inputLayer = new String(bytes);
		} else {
			inputLayer = null;
		}

		if (input.readBoolean()) {
			// Read the number of elements and their types
			int n = input.readByte();
			SimpleData[] signature = new SimpleData[n];
			for (int i = 0; i < n; ++i) {
				signature[i] = DataProvider.get().get(input.readByte());
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

		if (inputLayer != null) {
			byte[] bytes = inputLayer.getBytes();
			output.writeByte(bytes.length);
			output.write(bytes);
		}

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
	public void setInputLayer(Class<? extends InputLayer> clazz) {
		inputLayer = null;
		if (clazz == InputLayer.DEFAULT_LAYER) {
			buffer[32] = 0;
		} else if (clazz == BucketsLayer.class) {
			buffer[32] = 1;
		} else if (clazz == DummyLayer.class) {
			buffer[32] = 2;
		} else if (clazz == ChainSplitLayer.class) {
			buffer[32] = 3;
		} else {
			buffer[32] = -1;
			inputLayer = clazz.getName();
		}

	}

	@Override
	public Class<? extends InputLayer> getInputLayer() {
		switch (buffer[32]) {
		case 0:
			return InputLayer.DEFAULT_LAYER;
		case 1:
			return BucketsLayer.class;
		case 2:
			return DummyLayer.class;
		case 3:
			return ChainSplitLayer.class;
		default:
			try {
				return Class.forName(inputLayer).asSubclass(InputLayer.class);
			} catch (Throwable e) {
				throw new Error(
						"Could not load inputLayer class " + inputLayer, e);
			}
		}
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

	public int setActions(ActionContext context, ActionSequence actions)
			throws Exception {

		int retval = -1;

		if (actions != null) {
			for (int i = actions.length() - 1; i >= 0; i--) {
				ActionConf c = actions.get(i);
				int v = addAction(c, context);
				if (i == actions.length() - 1) {
					retval = v;
				}
			}
		} else {
			throw new Exception("No action is defined");
		}
		return retval;
	}

	private int addAction(ActionConf action, ActionContext context)
			throws Exception {

		int retval = -1;

		grow(bufferSize + 1024, true);

		// Process the parameters and possibly insert instructions to control
		// the flow.
		if (action.getConfigurator() != null) {
			controller.init();
			action.getConfigurator().process(this, action, controller, context);
			if (controller.doNotAddAction) {
				if (action.getConfigurator() != null
						&& controller.listActions.size() > 0) {
					// Add the actions from the last
					List<ActionConf> listToAdd = controller.listActions;
					controller.listActions = new ArrayList<ActionConf>();
					setActions(context, new ActionSequence(listToAdd));
				}
				return retval;
			}

			if (controller.stopProcessing) {
				cos.setCurrentPosition(bufferSize);
				cos.writeInt(controller.bucketId);
				cos.writeInt(controller.destination);
				cos.writeBoolean(true);
				bufferSize += 9;
			} else {
				cos.setCurrentPosition(bufferSize);
				cos.writeBoolean(false);
				bufferSize++;
			}
			retval = controller.bucketId;
		} else {
			cos.setCurrentPosition(bufferSize);
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
			List<ActionConf> listToAdd = controller.listActions;
			controller.listActions = new ArrayList<ActionConf>();
			setActions(context, new ActionSequence(listToAdd));
		}
		return retval;
	}

	void setRawSize(int size) {
		bufferSize = size;
	}

	void copyTo(Chain newChain) {
		newChain.bufferSize = bufferSize;
		newChain.grow(bufferSize, false);
		System.arraycopy(buffer, 0, newChain.buffer, 0, bufferSize);
		tuple.copyTo(newChain.tuple);
		newChain.inputLayer = inputLayer;
	}

	public void branch(Chain newChain, long newChainId, int skippingActions) {
		branch(newChain, newChainId, skippingActions, true);
	}

	public void branch(Chain newChain, long newChainId, int skippingActions,
			boolean incrementChild) {
		if (skippingActions > 0) {
			int originalSize = bufferSize;
			// Remove the first n actions
			while (skippingActions-- > 0 && bufferSize > CHAIN_RESERVED_SPACE) {
				bufferSize -= 4;
				int sizeNameAction = Utils.decodeInt(buffer, bufferSize);
				bufferSize -= 12 + sizeNameAction; // Skip also the chainID
				bufferSize -= Utils.decodeInt(buffer, bufferSize);
				if (buffer[--bufferSize] == 1) {
					bufferSize -= 8;
				}
			}
			copyTo(newChain);
			bufferSize = originalSize;
		} else {
			copyTo(newChain);
		}

		// Set up the new chain
		newChain.setParentChainId(this.getChainId());
		newChain.setChainId(newChainId);
		newChain.setTotalChainChildren(0);

		// Update counters of the new chain
		if (incrementChild)
			setTotalChainChildren(getTotalChainChildren() + 1);
	}

	long customBranch(Chain newChain, long newChainId, int startFromAction) {
		int sizeToCopy = CHAIN_RESERVED_SPACE;
		long parentChainId = 0;
		if (startFromAction != -1) {
			sizeToCopy = bufferSize;
			// Remove the first n actions
			while (startFromAction-- > 0 && sizeToCopy > CHAIN_RESERVED_SPACE) {
				sizeToCopy -= 4;
				int sizeNameAction = Utils.decodeInt(buffer, sizeToCopy);
				sizeToCopy -= 12 + sizeNameAction; // Skip also the chainID
				sizeToCopy -= Utils.decodeInt(buffer, sizeToCopy);
				if (buffer[--sizeToCopy] == 1) {
					sizeToCopy -= 8;
				}
			}
			// Get parentChainId
			if (sizeToCopy > CHAIN_RESERVED_SPACE) {
				int tmpSize = sizeToCopy - 4;
				int tNameAction = Utils.decodeInt(buffer, tmpSize);
				parentChainId = Utils.decodeLong(buffer, sizeToCopy - 12
						- tNameAction);
			}
		}

		newChain.bufferSize = sizeToCopy;
		if (newChain.buffer.length < buffer.length) {
			newChain.buffer = new byte[buffer.length];
		}
		System.arraycopy(buffer, 0, newChain.buffer, 0, sizeToCopy);

		// Set up the new chain
		newChain.setParentChainId(parentChainId);
		newChain.setChainId(newChainId);
		newChain.setTotalChainChildren(0);
		return parentChainId;
	}

	void getActions(ChainExecutor actions, ActionFactory ap) throws IOException {

		// Read the chain and feel the actions
		int tmpSize = bufferSize;
		long currentChainId = getChainId();
		boolean stopProcessing = false;
		int nodeId = 0;
		int bucketId = 0;

		while (tmpSize > CHAIN_RESERVED_SPACE && !stopProcessing) {
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

	int countActionsContaining(String s) {
		int tmpSize = bufferSize;
		int count = 0;
		while (tmpSize > CHAIN_RESERVED_SPACE) {
			int size = Utils.decodeInt(buffer, tmpSize - 4);
			String sAction = new String(buffer, tmpSize - size - 4, size);
			if (sAction.indexOf(s) != -1) {
				count++;
			}
			tmpSize -= Utils.decodeInt(buffer, tmpSize - 8 - size) + 4 + size;

		}
		return count;
	}

	public void grow(int needed, boolean copy) {
		if (needed > buffer.length) {
			int n = buffer.length;
			while (n < needed) {
				n += n;
			}
			byte[] newbuf = new byte[n];
			if (log.isDebugEnabled()) {
				log.debug("Growing buffer to " + n);
			}
			if (copy) {
				System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
			}
			buffer = newbuf;
			cis.setBuffer(newbuf);
			cos.setBuffer(newbuf);
		}
	}
}
