package arch.actions;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.data.types.Tuple;

public class Sample extends Action {

	static final Logger log = LoggerFactory.getLogger(Sample.class);

	public static final int SAMPLE_RATE = 0;
	public static final String S_SAMPLE_RATE = "sample_rate";

	Tuple tuple = new Tuple();
	int sampling;
	Random rand = new Random();

	@Override
	public void setupActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(SAMPLE_RATE, S_SAMPLE_RATE, null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		sampling = getParamInt(SAMPLE_RATE);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context, Output output)
			throws Exception {
		if (rand.nextInt(100) < sampling) {
			output.output(inputTuple);
		}
	}
}
