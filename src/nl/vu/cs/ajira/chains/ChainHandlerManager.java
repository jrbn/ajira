package nl.vu.cs.ajira.chains;

import ibis.util.ThreadPool;

import java.util.Set;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.storage.containers.CheckedConcurrentWritableContainer;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainHandlerManager {

	static final Logger log = LoggerFactory
			.getLogger(ChainHandlerManager.class);

	private static ChainHandlerManager manager = new ChainHandlerManager();

	private Context context;
	private WritableContainer<Chain> chainsToProcess = new CheckedConcurrentWritableContainer<Chain>(
			Consts.SIZE_BUFFERS_CHAINS_PROCESS);

	// Statistics
	private Set<ChainHandler> chainHandlers = new ConcurrentHashSet<ChainHandler>();
	private int activeHandlers = 0;
	private int inactiveHandlers = 0;
	private int waitHandlers = 0;

	public static ChainHandlerManager getInstance() {
		return manager;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public void startChainHandlers(int nChainHandlers) {
		for (int j = 0; j < nChainHandlers; ++j) {
			log.debug("Starting Chain Handler " + j + " ...");
			ChainHandler handler = new ChainHandler(context);
			Thread thread = new Thread(handler);
			thread.setName("Chain Handler");
			thread.start();
			chainHandlers.add(handler);
		}
	}

	public void startSeparateChainHandler(Chain chain) {
		ChainHandler handler = new ChainHandler(context, chain);
		ThreadPool.createNew(handler, "Chain Handler");
		chainHandlers.add(handler);
	}

	public WritableContainer<Chain> getChainsToProcess() {
		return chainsToProcess;
	}

	public int getActiveChainHandlers() {
		return activeHandlers;
	}

	public int getInactiveChainHandlers() {
		return inactiveHandlers;
	}

	public int getWaitChainHandlers() {
		return waitHandlers;
	}

	public void doHouseKeeping() {
		activeHandlers = 0;
		inactiveHandlers = 0;
		waitHandlers = 0;

		for (ChainHandler handler : chainHandlers) {
			if (handler.getStatus() == ChainHandler.STATUS_INACTIVE) {
				inactiveHandlers++;
			} else if (handler.getStatus() == ChainHandler.STATUS_ACTIVE) {
				activeHandlers++;
			} else if (handler.getStatus() == ChainHandler.STATUS_WAIT) {
				waitHandlers++;
			} else if (handler.getStatus() == ChainHandler.STATUS_FINISHED) {
				chainHandlers.remove(handler);
			}
		}

		// if all threads are waiting and there are elements in the chain there
		// is a lock to fix. Start a new chain...
		if (waitHandlers == chainHandlers.size()
				&& chainsToProcess.getNElements() > 0) {
			ChainHandler handler = new ChainHandler(context);
			Thread thread = new Thread(handler);
			thread.setName("Chain Handler");
			thread.start();
			chainHandlers.add(handler);
		}

	}
}
