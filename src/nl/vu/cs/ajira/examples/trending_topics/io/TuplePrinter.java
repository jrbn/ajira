package nl.vu.cs.ajira.examples.trending_topics.io;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TLongArray;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TuplePrinter extends Action {
	public static final int S_FILENAME = 0;
	private static final int printEvery = 10000;

	private final Logger logger = LoggerFactory.getLogger(TuplePrinter.class);
	private int numProcessed = 0;
	private long startTime;
	private BufferedWriter out;

	public static void addToChain(String filename, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(TuplePrinter.class);
		c.setParamString(S_FILENAME, filename);
		actions.add(c);
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_FILENAME, "filename", "default.txt", true);
	}

	@Override
	public void startProcess(ActionContext context) {
		numProcessed = 0;
		startTime = System.currentTimeMillis();
		try {
			out = new BufferedWriter(new FileWriter(getParamString(S_FILENAME)));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (logger.isDebugEnabled()) {
			logger.debug("Received tuple: " + printTupleContent(tuple));
		}
		numProcessed++;
		if ((numProcessed % printEvery) == 0) {
			long runningTime = System.currentTimeMillis() - startTime;
			String line = String.valueOf(runningTime)
					+ "\t"
					+ String.valueOf(numProcessed)
					+ "\t"
					+ String.valueOf((double) numProcessed * 1000 / runningTime)
					+ "\n";
			out.append(line);
			out.flush();
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (logger.isDebugEnabled()) {
			logger.debug("Processed " + numProcessed + " tuples");
		}
	}

	private final String printTupleContent(Tuple tuple) {
		StringBuilder builder = new StringBuilder();
		String[] stringArray = ((TStringArray) tuple.get(0)).getArray();
		long[] countArray = ((TLongArray) tuple.get(1)).getArray();
		assert (stringArray.length == countArray.length);
		builder.append("[");
		for (int i = 0; i < stringArray.length; i++) {
			String str = stringArray[i];
			long count = countArray[i];
			if (i != 0) {
				builder.append(" | ");
			}
			builder.append(str + " -> " + count);
		}
		builder.append("]");
		return builder.toString();
	}
}
