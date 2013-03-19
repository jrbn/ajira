package nl.vu.cs.ajira.actions;

import java.io.File;

import nl.vu.cs.ajira.actions.support.FilterHiddenFiles;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.datalayer.files.FileCollection;
import nl.vu.cs.ajira.datalayer.files.FileLayer;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFromFiles extends Action {

	public static final String MINIMUM_SPLIT_SIZE = "splitinput.minimumsize";
	public static final int MINIMUM_FILE_SPLIT = (4 * 1024 * 1024); // 4 MB

	public static final int S_PATH = 0;
	public static final int S_CUSTOM_READER = 1;
	public static final int S_FILE_FILTER = 2;

	static final Logger log = LoggerFactory.getLogger(ReadFromFiles.class);

	private int minimumFileSplitSize;
	private FileCollection currentFileSplit;
	private int splitId;
	private String customReader = null;

	static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context) {
			if (params[S_PATH] != null) {
				query.setInputLayer(Consts.DEFAULT_INPUT_LAYER_ID);
				query.setQuery(new Query(new TInt(FileLayer.OP_LS),
						new TString((String) params[S_PATH]), new TString(
								(String) params[S_FILE_FILTER])));
			}
		}
	}

	private void processSplit(ActionContext context, ActionOutput output)
			throws Exception {
		String key = "split-" + splitId++;
		context.putObjectInCache(key, currentFileSplit);

		Tuple tuple = TupleFactory.newTuple();
		if (customReader == null) {
			tuple.set(new TInt(FileLayer.OP_READ), new TString(key), new TInt(
					context.getMyNodeId()));
		} else {
			tuple.set(new TInt(FileLayer.OP_READ), new TString(key), new TInt(
					context.getMyNodeId()), new TString(customReader));
		}

		ActionConf c = ActionFactory.getActionConf(QueryInputLayer.class);
		c.setParamWritable(QueryInputLayer.W_QUERY, new Query(tuple));
		output.branch(c);

		currentFileSplit = new FileCollection();
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_PATH, "path", null, true);
		conf.registerParameter(S_CUSTOM_READER, "custom reader", null, false);
		conf.registerParameter(S_FILE_FILTER, "Filter",
				FilterHiddenFiles.class.getName(), false);
		conf.registerCustomConfigurator(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		customReader = getParamString(S_CUSTOM_READER);
		minimumFileSplitSize = context.getSystemParamInt(MINIMUM_SPLIT_SIZE,
				MINIMUM_FILE_SPLIT);
		currentFileSplit = new FileCollection();
		splitId = 0;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {

		// In input I receive a list of files
		File file = new File(((TString) inputTuple.get(0)).getValue());

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
