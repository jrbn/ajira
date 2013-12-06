package nl.vu.cs.ajira.examples.trending_topics.actions;

import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TLongArray;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.examples.trending_topics.tools.RankableString;
import nl.vu.cs.ajira.examples.trending_topics.tools.Rankings;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract action provides the basic behavior of bolts that rank objects
 * according to their count.
 * <p/>
 * It uses a template method design pattern for
 * {@link AbstractRankerAction#execute(Tuple, BasicOutputCollector)} to allow
 * actual action implementations to specify how incoming tuples are processed,
 * i.e. how the objects embedded within those tuples are retrieved and counted.
 */
public abstract class AbstractRankerAction extends Action implements Runnable {
	public static final int I_TOP_N = 0;
	public static final int I_EMIT_FREQUENCY_IN_SECONDS = 1;
	public static final int B_EVAL_MODE = 2;

	protected final Logger logger = LoggerFactory
			.getLogger(AbstractRankerAction.class);

	private int topN;
	private int emitFreq;
	private boolean evalMode;

	private Rankings rankings;
	private volatile boolean needsToEmit;
	private volatile boolean stopClock;
	private Thread clock;

	public static void addIntermediateRankingToChain(int topN, int emitFreq,
			boolean evalMode, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory
				.getActionConf(IntermediateRankingsAction.class);
		c.setParamInt(I_TOP_N, topN);
		c.setParamInt(I_EMIT_FREQUENCY_IN_SECONDS, emitFreq);
		c.setParamBoolean(B_EVAL_MODE, evalMode);
		actions.add(c);
	}

	public static void addTotalRankingToChain(int topN, int emitFreq,
			boolean evalMode, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(TotalRankingsAction.class);
		c.setParamInt(I_TOP_N, topN);
		c.setParamInt(I_EMIT_FREQUENCY_IN_SECONDS, emitFreq);
		c.setParamBoolean(B_EVAL_MODE, evalMode);
		actions.add(c);
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_TOP_N, "top n", 1, true);
		conf.registerParameter(I_EMIT_FREQUENCY_IN_SECONDS, "emit frequency",
				1, true);
		conf.registerParameter(
				B_EVAL_MODE,
				"evaluation mode (ranking is recomputed for every incoming word",
				1, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		topN = getParamInt(I_TOP_N);
		emitFreq = getParamInt(I_EMIT_FREQUENCY_IN_SECONDS);
		evalMode = getParamBoolean(B_EVAL_MODE);

		needsToEmit = false;
		stopClock = false;
		rankings = new Rankings(topN);
		clock = new Thread(this);
		clock.start();
	}

	protected Rankings getRankings() {
		return rankings;
	}

	/**
	 * This method functions as a template method (design pattern).
	 */
	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		updateRankingsWithTuple(tuple);
		if (needsToEmit || evalMode) {
			if (log.isDebugEnabled()) {
				logger.debug("Emit current rankings");
			}
			emitRankings(actionOutput);
			needsToEmit = false;
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		stopClock = true;
	}

	abstract void updateRankingsWithTuple(Tuple tuple);

	private final void emitRankings(ActionOutput actionOutput) throws Exception {
		if (log.isDebugEnabled()) {
			logger.debug("Rankings: " + rankings);
		}
		List<RankableString> rankableList = rankings.getRankings();
		if (rankableList.isEmpty())
			return;
		String[] strArray = new String[rankings.size()];
		long[] countArray = new long[rankings.size()];
		int i = 0;
		for (RankableString r : rankableList) {
			String str = r.getString();
			long count = r.getCount();
			strArray[i] = str;
			countArray[i] = count;
			i++;
		}
		actionOutput.output(new TStringArray(strArray), new TLongArray(
				countArray));
	}

	@Override
	public void run() {
		while (!stopClock) {
			try {
				Thread.sleep(emitFreq * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			needsToEmit = true;
		}
	}

}
