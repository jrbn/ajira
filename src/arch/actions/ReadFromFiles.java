package arch.actions;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.actions.support.FilterHiddenFiles;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.datalayer.Query;
import arch.datalayer.files.FileCollection;
import arch.datalayer.files.FileLayer;
import arch.utils.Consts;

public class ReadFromFiles extends Action {

	public static final String MINIMUM_SPLIT_SIZE = "splitinput.minimumsize";
	public static final int MINIMUM_FILE_SPLIT = (256 * 1024 * 1024); // 256 MB

	public static final int PATH = 0;
	public static final String S_PATH = "path";
	public static final int CUSTOM_READER = 1;
	public static final String S_CUSTOM_READER = "custom_reader";

	static final Logger log = LoggerFactory.getLogger(ReadFromFiles.class);

	private int minimumFileSplitSize;
	private FileCollection currentFileSplit;
	private int splitId;
	private String customReader = null;

	static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		void setupConfiguration(Query query, Object[] params,
				ActionController controller, ActionContext context)
				throws Exception {
			if (params[PATH] != null) {
				query.setInputLayer(Consts.DEFAULT_INPUT_LAYER_ID);
				query.setInputTuple(new Tuple(new TInt(FileLayer.OP_LS),
						new TString((String) params[PATH]), new TString(
								FilterHiddenFiles.class.getName())));
			}
		}
	}

	private void processSplit(ActionContext context, ActionOutput output)
			throws Exception {
		String key = "split-" + splitId++;
		context.putObjectInCache(key, currentFileSplit);

		Tuple tuple = new Tuple();
		if (customReader == null) {
			tuple.set(new TInt(FileLayer.OP_READ), new TString(key), new TInt(
					context.getMyNodeId()));
		} else {
			tuple.set(new TInt(FileLayer.OP_READ), new TString(key), new TInt(
					context.getMyNodeId()), new TString(customReader));
		}

		ActionConf c = ActionFactory.getActionConf(QueryInput.class);
		c.setParam(QueryInput.TUPLE, tuple);
		output.branch(c);

		currentFileSplit = new FileCollection();
	}

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(PATH, S_PATH, null, true);
		conf.registerParameter(CUSTOM_READER, S_CUSTOM_READER, null, false);
		conf.registerCustomConfigurator(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		customReader = getParamString(CUSTOM_READER);
		minimumFileSplitSize = context.getSystemParamInt(MINIMUM_SPLIT_SIZE,
				MINIMUM_FILE_SPLIT);
		currentFileSplit = new FileCollection();
		splitId = 0;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {

		// In input I receive a list of files
		TString path = new TString();
		inputTuple.get(path, 0);
		File file = new File(path.getValue());

		long sizeFile = file.length();
		if ((currentFileSplit.getSize() + sizeFile) >= minimumFileSplitSize) {
			processSplit(context, output);
		}
		currentFileSplit.addFile(file);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		if (currentFileSplit.getSize() > 0) {
			processSplit(context, output);
		}
		context.incrCounter("# file splits", splitId);
	}

}
