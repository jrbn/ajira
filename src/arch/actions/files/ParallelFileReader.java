package arch.actions.files;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.datalayer.files.DefaultFileParser;
import arch.datalayer.files.FileCollection;
import arch.datalayer.files.FileLayer;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class ParallelFileReader extends Action {

	public static final String MINIMUM_SPLIT_SIZE = "splitinput.minimumsize";
	public static final int MINIMUM_FILE_SPLIT = 4 * 1024 * 1024; // 1 MB

	static final Logger log = LoggerFactory.getLogger(ParallelFileReader.class);

	private final Tuple tuple = new Tuple();
	private int minimumFileSplitSize;
	private FileCollection currentFileSplit;
	private int splitId;
	private String customReader = null;

	@Override
	public void readFrom(DataInput input) throws IOException {
		int l = input.readByte();
		if (l != -1) {
			byte[] content = new byte[l];
			input.readFully(content);
			customReader = new String(content);
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		if (customReader == null) {
			output.writeByte(-1);
		} else {
			byte[] c = customReader.getBytes();
			output.writeByte(c.length);
			output.write(c);
		}
	}

	@Override
	public int bytesToStore() {
		return customReader == null ? 1 : 1 + customReader.getBytes().length;
	}

	private Chain processSplit(ActionContext context, Chain chain,
			WritableContainer<Chain> chainsToProcess) throws Exception {
		String key = "split-" + splitId++;
		context.putObjectInCache(key, currentFileSplit);

		Chain newChain = new Chain();
		chain.createBranch(context, newChain);

		if (customReader == null) {
			tuple.set(new TInt(FileLayer.OP_READ), new TString(key), new TInt(
					context.getNetworkLayer().getMyPartition()));
		} else {
			tuple.set(new TInt(FileLayer.OP_READ), new TString(key), new TInt(
					context.getNetworkLayer().getMyPartition()), new TString(
					customReader));
		}

		newChain.replaceInputTuple(tuple);
		newChain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);

		chainsToProcess.add(newChain);
		currentFileSplit = new FileCollection();

		return newChain;
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		minimumFileSplitSize = context.getConfiguration().getInt(
				MINIMUM_SPLIT_SIZE, MINIMUM_FILE_SPLIT);
		currentFileSplit = new FileCollection();
		splitId = 0;
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception {

		// In input I receive a list of files
		TString path = new TString();
		inputTuple.get(path, 0);
		File file = new File(path.getValue());

		long sizeFile = file.length();
		if (currentFileSplit.getSize() + sizeFile >= minimumFileSplitSize) {
			processSplit(context, chain, chainsToProcess);
		}
		currentFileSplit.addFile(file);
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToSend) throws Exception {
		if (currentFileSplit.getSize() > 0) {
			processSplit(context, chain, chainsToSend);
		}
		context.incrCounter("# file splits", splitId);
	}

	public void setCustomIteratorClass(Class<? extends DefaultFileParser> class1) {
		customReader = class1.getName();
	}
}
