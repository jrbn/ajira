package nl.vu.cs.ajira.mgmt;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.chains.ChainHandlerManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeHouseKeeper implements Runnable {

	static final Logger log = LoggerFactory.getLogger(NodeHouseKeeper.class);

	private ChainHandlerManager manager;
	private StatisticsCollector stats;
	private boolean local;

	public NodeHouseKeeper(Context context) {
		this.manager = context.getChainHandlerManager();
		this.local = context.isLocalMode();
		this.stats = context.getStatisticsCollector();
	}

	@Override
	public void run() {
		while (true) {
			try {
				manager.doHouseKeeping();
				if (!local) {
					stats.sendStatisticsAway();
				}
				Thread.sleep(500);
			} catch (Exception e) {
				log.error("Error", e);
			}
		}
	}
}
