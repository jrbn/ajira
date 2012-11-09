package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class Sample extends Action {

	static final Logger log = LoggerFactory.getLogger(Sample.class);

	Tuple tuple = new Tuple();
	int sampling;
	Random rand = new Random();

	public void setSample(int sample) {
		sampling = sample;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		sampling = input.readInt();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(sampling);
	}

	@Override
	public int bytesToStore() {
		return 4;
	}

	@Override
	public void process(Tuple inputTuple, Chain remainingChain,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess,
			WritableContainer<Tuple> output, ActionContext context)
			throws Exception {
		if (rand.nextInt(100) < sampling) {
			output.add(inputTuple);
		}
	}
}
