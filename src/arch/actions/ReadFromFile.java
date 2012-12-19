package arch.actions;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.support.FilterHiddenFiles;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.datalayer.files.FileCollection;
import arch.datalayer.files.FileLayer;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class ReadFromFile extends Action {

	public static final String MINIMUM_SPLIT_SIZE = "splitinput.minimumsize";
	public static final int MINIMUM_FILE_SPLIT = 4 * 1024 * 1024; // 1 MB

	public static final int PATH = 0;
	public static final String S_PATH = "path";
	public static final int CUSTOM_READER = 1;
	public static final String S_CUSTOM_READER = "custom_reader";

	static final Logger log = LoggerFactory.getLogger(ReadFromFile.class);

	private final Tuple tuple = new Tuple();
	private int minimumFileSplitSize;
	private FileCollection currentFileSplit;
	private int splitId;
	private String customReader = null;

	static class ParametersProcessor extends
			ActionConf.RuntimeParameterProcessor {
		@Override
		void processParameters(Chain chain, Object[] params,
				ActionContext context) {
			if (params[PATH] != null) {
				chain.setInputTuple(new Tuple(new TInt(FileLayer.OP_LS),
						new TString((String) params[PATH]), new TString(
								FilterHiddenFiles.class.getName())));
			}
		}
	}

	private Chain processSplit(ActionContext context, Chain chain,
			WritableContainer<Chain> chainsToProcess) throws Exception {
		String key = "split-" + splitId++;
		context.putObjectInCache(key, currentFileSplit);

		Chain newChain = new Chain();
		chain.branch(newChain);

		if (customReader == null) {
			tuple.set(new TInt(FileLayer.OP_READ), new TString(key), new TInt(
					context.getMyNodeId()));
		} else {
			tuple.set(new TInt(FileLayer.OP_READ), new TString(key), new TInt(
					context.getMyNodeId()), new TString(customReader));
		}

		newChain.setInputTuple(tuple);
		newChain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);

		chainsToProcess.add(newChain);
		currentFileSplit = new FileCollection();

		return newChain;
	}

	@Override
	public void setupActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(PATH, S_PATH, null, true);
		conf.registerParameter(CUSTOM_READER, S_CUSTOM_READER, null, false);
		conf.registerRuntimeParameterProcessor(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		customReader = getParamString(CUSTOM_READER);
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

}
