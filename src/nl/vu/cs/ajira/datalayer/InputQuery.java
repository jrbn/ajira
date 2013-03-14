package nl.vu.cs.ajira.datalayer;

import nl.vu.cs.ajira.actions.support.Query;

public interface InputQuery {

	public void setQuery(Query tuple);

	public void getQuery(Query tuple);

	public void setInputLayer(int inputLayerID);

	public int getInputLayer();
}