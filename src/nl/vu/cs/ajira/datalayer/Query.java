package nl.vu.cs.ajira.datalayer;

import nl.vu.cs.ajira.data.types.Tuple;

public interface Query {

	public void setInputTuple(Tuple tuple);

	public void getInputTuple(Tuple tuple);

	public void setInputLayer(int inputLayerID);

	public int getInputLayer();
}