package nl.vu.cs.ajira.actions;

public interface ActionController {

	public void continueComputationOn(int destination, int bucketId);

	public void doNotAddAction();
}
