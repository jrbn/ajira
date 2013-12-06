package nl.vu.cs.ajira.examples.trending_topics.actions;

import java.util.Map;
import java.util.Map.Entry;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.examples.trending_topics.tools.NthLastModifiedTimeTracker;
import nl.vu.cs.ajira.examples.trending_topics.tools.SlidingWindowCounter;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This action performs rolling counts of incoming objects, i.e. sliding window
 * based counting.
 * <p/>
 * The action is configured by two parameters, the length of the sliding window
 * in number of tuples (which influences the output data of the action, i.e. how
 * it will count objects) and the emit frequency in number of tuples (which
 * influences how often the action will output the latest window counts).
 * <p/>
 * The action emits a rolling count tuple per object, consisting of the object
 * itself and its latest rolling count, and the actual duration of the sliding
 * window.
 */
public class RollingCountAction extends Action implements Runnable {
	public static final int I_SLIDING_WINDOW_IN_SECONDS = 0;
	public static final int I_EMIT_FREQUENCY_IN_SECONDS = 1;
	public static final int B_EVAL_MODE = 2;

	private final Logger logger = LoggerFactory
			.getLogger(RollingCountAction.class);

	private int windowLength;
	private int emitFrequency;
	private boolean evalMode;

	private SlidingWindowCounter<String> counter;
	private volatile boolean needsToEmit;
	private volatile boolean stopClock;
	private Thread clock;
	private NthLastModifiedTimeTracker lastModifiedTracker;

	public static void addToChain(int slidingWin, int emitFreq,
			boolean evalMode, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(RollingCountAction.class);
		c.setParamInt(I_SLIDING_WINDOW_IN_SECONDS, slidingWin);
		c.setParamInt(I_EMIT_FREQUENCY_IN_SECONDS, emitFreq);
		c.setParamBoolean(B_EVAL_MODE, evalMode);
		actions.add(c);
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_SLIDING_WINDOW_IN_SECONDS,
				"sliding window (seconds)", 1, true);
		conf.registerParameter(I_EMIT_FREQUENCY_IN_SECONDS,
				"emit frequency (seconds)", 1, true);
		conf.registerParameter(
				B_EVAL_MODE,
				"evaluation mode (ranking is recomputed for every incoming word",
				1, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		windowLength = getParamInt(I_SLIDING_WINDOW_IN_SECONDS);
		emitFrequency = getParamInt(I_EMIT_FREQUENCY_IN_SECONDS);
		evalMode = getParamBoolean(B_EVAL_MODE);

		lastModifiedTracker = new NthLastModifiedTimeTracker(
				deriveNumWindowChunksFrom(windowLength, emitFrequency));
		needsToEmit = false;
		stopClock = false;
		counter = new SlidingWindowCounter<String>(deriveNumWindowChunksFrom(
				windowLength, emitFrequency));
		clock = new Thread(this);
		clock.start();
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		countObj(tuple);
		if (needsToEmit || evalMode) {
			if (logger.isDebugEnabled()) {
				logger.debug("Emit current window counts");
			}
			emitCurrentWindowCounts(actionOutput);
			needsToEmit = false;
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		stopClock = true;
	}

	private final void emitCurrentWindowCounts(ActionOutput output)
			throws Exception {
		lastModifiedTracker.markAsModified();
		Map<String, Long> counts = counter.getCountsThenAdvanceWindow();
		for (Entry<String, Long> entry : counts.entrySet()) {
			String str = entry.getKey();
			Long count = entry.getValue();
			Tuple tuple = TupleFactory.newTuple(new TString(str), new TLong(
					count));
			output.output(tuple);
		}
	}

	private final void countObj(Tuple tuple) {
		String str = ((TString) tuple.get(0)).getValue();
		counter.incrementCount(str);
	}

	private final int deriveNumWindowChunksFrom(int windowLengthInSeconds,
			int windowUpdateFrequencyInSeconds) {
		return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
	}

	@Override
	public void run() {
		while (!stopClock) {
			try {
				Thread.sleep(emitFrequency * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			needsToEmit = true;
		}
	}

}
