package nl.vu.cs.ajira.actions;

import java.util.Random;

import nl.vu.cs.ajira.data.types.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sample extends Action {

	static final Logger log = LoggerFactory.getLogger(Sample.class);

	public static final int SAMPLE_RATE = 0;
	public static final String S_SAMPLE_RATE = "SAMPLE_RATE";

	int sampling;
	Random rand = new Random();

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(SAMPLE_RATE, S_SAMPLE_RATE, null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		sampling = getParamInt(SAMPLE_RATE);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (rand.nextInt(100) < sampling) {
			output.output(inputTuple);
		}
	}
}
