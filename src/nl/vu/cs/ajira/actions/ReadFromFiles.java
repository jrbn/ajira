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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The
 * <code>ReadFromFiles<code> action uses the input layer to obtain a list of files,
 * spits them up into chunks of a certain size, and divides these over the available
 * nodes, to read those files. A next action gets the tuples read from these files as
 * input. Note that all this may cause chains to be executed on all available nodes.
 */
public class ReadFromFiles extends Action {

	// TODO: Why is this not a configuration parameter of the action, but a
	// configuration parameter
	// of the action context? --Ceriel
	public static final String MINIMUM_SPLIT_SIZE = "splitinput.minimumsize";
	public static final int MINIMUM_FILE_SPLIT = (4 * 1024 * 1024); // 4 MB

	/**
	 * The <code>S_PATH</code> parameter, of type <code>String</code>, is
	 * required and indicates the directory containing the files to be read.
	 */
	public static final int S_PATH = 0;

	/**
	 * The <code>S_CUSTOM_READER</code> parameter, of type <code>String</code>,
	 * is not required, and, if provided, should contain the class name of a
	 * subclass of {@link nl.vu.cs.ajira.datalayer.files.FileReader}. This
	 * subclass should have a public constructor with a single
	 * {@link java.io.File} parameter.
	 */
	public static final int S_CUSTOM_READER = 1;

	/**
	 * The <code>S_FILE_FILTER</code> parameter, of type <code>String</code>, is
	 * not required, and, if provided, should contain the class name of an
	 * implementation of the {@link java.io.FilenameFilter} interface. This
	 * class should have a public parameterless constructor.
	 */
	public static final int S_FILE_FILTER = 2;

	static final Logger log = LoggerFactory.getLogger(ReadFromFiles.class);

	private static int splitCounter = 0;	// Added to create unique strings for the context cache. --Ceriel
	private int minimumFileSplitSize;
	private FileCollection currentFileSplit;
	private int splitId;
	private String baseSplit = null;
	private String customReader = null;

	private static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context) {
			if (params[S_PATH] != null) {
				query.setInputLayer(FileLayer.class);
				query.setQuery(new Query(new TInt(FileLayer.OP_LS),
						new TString((String) params[S_PATH]), new TString(
								(String) params[S_FILE_FILTER])));
			}
		}
	}

	private void processSplit(ActionContext context, ActionOutput output)
			throws Exception {
		if (baseSplit == null) {
			synchronized(this.getClass()) {
				baseSplit = "split" + splitCounter++ + "-";  
			}
		}
		String key = baseSplit + splitId++;
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
		c.setParamString(QueryInputLayer.S_INPUTLAYER,
				FileLayer.class.getName());
		c.setParamWritable(QueryInputLayer.W_QUERY, new Query(tuple));
		c.setParamStringArray(QueryInputLayer.SA_SIGNATURE_QUERY,
				tuple.getSignature());
		output.branch(new ActionSequence(c));

		currentFileSplit = new FileCollection();
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_PATH, "S_PATH", null, true);
		conf.registerParameter(S_CUSTOM_READER, "S_CUSTOM_READER", null, false);
		conf.registerParameter(S_FILE_FILTER, "S_FILE_FILTER",
				FilterHiddenFiles.class.getName(), false);
		conf.registerCustomConfigurator(new ParametersProcessor());
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
		if (currentFileSplit.getNFiles() > 0 && (currentFileSplit.getSize() + sizeFile) >= minimumFileSplitSize) {
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
