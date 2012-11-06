package arch.test;

import arch.actions.Partitioner;
import arch.data.types.TString;
import arch.data.types.Tuple;

public class LinePartitioner extends Partitioner {

	TString line = new TString();

	@Override
	protected int partition(Tuple tuple, int nnodes) throws Exception {
		tuple.get(line);
		return (line.getValue().hashCode() & Integer.MAX_VALUE) % nnodes;
	}
}
