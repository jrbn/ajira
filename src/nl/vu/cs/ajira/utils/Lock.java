package nl.vu.cs.ajira.utils;

final public class Lock {
	private int count = 0;

	synchronized final public void increase() {
		count++;
		this.notifyAll();
	}
	
	public synchronized int getCount() {
		return count;
	}
}
