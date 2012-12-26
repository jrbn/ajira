package arch.datalayer;

import arch.data.types.Tuple;

public interface Query {

	public void setInputTuple(Tuple tuple);

	public void getInputTuple(Tuple tuple);

	public void setInputLayer(int inputLayerID);

	public int getInputLayer();
}