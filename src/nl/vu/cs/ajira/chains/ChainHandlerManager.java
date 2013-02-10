package nl.vu.cs.ajira.chains;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.storage.containers.CheckedConcurrentWritableContainer;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainHandlerManager {

	static final Logger log = LoggerFactory
			.getLogger(ChainHandlerManager.class);

	private static ChainHandlerManager manager = new ChainHandlerManager();

	private Context context;
	private WritableContainer<Chain> chainsToProcess = new CheckedConcurrentWritableContainer<Chain>(
			Consts.SIZE_BUFFERS_CHAINS_PROCESS);

	public static ChainHandlerManager getInstance() {
		return manager;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public void startChainHandlers(int i) {
		for (int j = 0; j < i; ++j) {
			log.debug("Starting Chain Handler " + j + " ...");
			ChainHandler handler = new ChainHandler(context);
			Thread thread = new Thread(handler);
			thread.setName("Chain Handler " + j);
			thread.start();
		}
	}

	public WritableContainer<Chain> getChainsToProcess() {
		return chainsToProcess;
	}

}
