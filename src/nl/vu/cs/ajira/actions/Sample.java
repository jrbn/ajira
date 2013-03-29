package nl.vu.cs.ajira.actions;

import java.util.Random;

import nl.vu.cs.ajira.data.types.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>Sample</code> action samples its input: a specified percentage of the
 * input tuples is passed on to the {@link ActionOutput}. Which tuples are passed
 * on is determined by a call to {@link Random#nextInt(int) Random.nextInt(100)}.
 * If the result of this call is less than the specified percentage, the input tuple
 * is passed on.
 */
public class Sample extends Action {

	static final Logger log = LoggerFactory.getLogger(Sample.class);

	/**
	 * The <code>I_SAMPLE_RATE</code> parameter, of type <code>int</code> is required,
	 * and should be a number between 1 and 100, representing the percentage of tuples
	 * that should be passed on.
	 */
	public static final int I_SAMPLE_RATE = 0;

	private int sampling;
	private final Random rand = new Random();

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_SAMPLE_RATE, "I_SAMPLE_RATE", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		sampling = getParamInt(I_SAMPLE_RATE);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (rand.nextInt(100) < sampling) {
			output.output(inputTuple);
		}
	}
}
