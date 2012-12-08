package arch.actions;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

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
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		sampling = getParamInt(SAMPLE_RATE);
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception {
		if (rand.nextInt(100) < sampling) {
			output.add(inputTuple);
		}
	}
}
