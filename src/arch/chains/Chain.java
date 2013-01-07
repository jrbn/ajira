package arch.chains;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.actions.Action;
import arch.actions.ActionConf;
import arch.actions.ActionContext;
import arch.actions.ActionFactory;
import arch.data.types.Tuple;
import arch.data.types.bytearray.BDataInput;
import arch.data.types.bytearray.BDataOutput;
import arch.datalayer.Query;
import arch.storage.Writable;
import arch.utils.Consts;
import arch.utils.Utils;

/**
 * 
 * 8 bytes: submission ID the chain belongs to 8 bytes: chain ID 8 bytes: parent
 * chain ID 4 bytes: n children 4 bytes: replicated factor 1 byte: input layer
 * to consider (0 is the default) 1 byte: flag to exclude the execution of the
 * fixed chain
 * 
 * @author jacopo
 * 
 */

public class Chain extends Writable implements Query {

	static final Logger log = LoggerFactory.getLogger(Chain.class);

	private int startingPosition = Consts.CHAIN_RESERVED_SPACE;
	private int bufferSize = Consts.CHAIN_RESERVED_SPACE;

	private final byte[] buffer = new byte[Consts.CHAIN_SIZE];
	private Tuple inputTuple = null;

	private final BDataOutput cos = new BDataOutput(buffer);
	private final BDataInput cis = new BDataInput(buffer);

	@Override
	public void readFrom(DataInput input) throws IOException {
		startingPosition = input.readInt();
		bufferSize = input.readInt();
		input.readFully(buffer, 0, bufferSize);

		if (input.readBoolean()) {
			if (inputTuple == null) {
				inputTuple = new Tuple();
			}
			inputTuple.readFrom(input);
		} else {
			inputTuple = null;
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(startingPosition);
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
	public int bytesToStore() {
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

	public void setTotalChainChildren(int chainChildren) {
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

	void addAction(ActionConf params, ActionContext context) throws Exception {

		// Validate the action
		if (!params.validateParameters()) {
			throw new Exception("Some required parameters for the action "
					+ params.getClassName() + " are not set.");
		}

		if (params.isParProcessorDefined()) {
			params.getRuntimeParametersProcessor().process(this, params,
					context);
		}

		int totalSize = bufferSize;

		// Serialize the action configuration
		cos.setCurrentPosition(bufferSize);
		params.writeTo(cos);
		int sizeAction = cos.cb.end - totalSize;
		bufferSize += sizeAction;
		Utils.encodeInt(buffer, bufferSize, sizeAction);
		bufferSize += 4;

		// Serialize the chain id of this chain
		Utils.encodeLong(buffer, bufferSize, getChainId());
		bufferSize += 8;

		// Serialize the class name
		byte[] sAction = params.getClassName().getBytes();
		System.arraycopy(sAction, 0, buffer, bufferSize, sAction.length);
		bufferSize += sAction.length;
		Utils.encodeInt(buffer, bufferSize, sAction.length);
		bufferSize += 4;
	}

	void setRawSize(int size) {
		bufferSize = size;
	}

	private void copyTo(Chain newChain) {
		newChain.startingPosition = startingPosition;
		newChain.bufferSize = bufferSize;
		System.arraycopy(buffer, 0, newChain.buffer, 0, bufferSize);
		if (inputTuple != null) {
			if (newChain.inputTuple == null) {
				newChain.inputTuple = new Tuple();
			}
			inputTuple.copyTo(newChain.inputTuple);
		} else {
			newChain.inputTuple = null;
		}
	}

	public void branch(Chain newChain, long newChainId) {
		copyTo(newChain);
		newChain.setParentChainId(this.getChainId());
		newChain.setChainId(newChainId);

		newChain.setTotalChainChildren(0);

		setTotalChainChildren(getTotalChainChildren() + 1);
	}

	void getActions(ActionsExecutor actions, ActionFactory ap)
			throws IOException {

		// Read the chain and feel the actions
		int tmpSize = bufferSize;
		long currentChainId = getChainId();

		while (tmpSize > startingPosition) {
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

			actions.addAction(action, chainId == currentChainId, tmpSize);
		}
	}
}
