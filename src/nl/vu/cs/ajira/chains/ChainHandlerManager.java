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
	private final WritableContainer<Chain> chainsToProcess = new CheckedConcurrentWritableContainer<Chain>(
			Consts.SIZE_BUFFERS_CHAINS_PROCESS);

	// Statistics
	private final Set<ChainHandler> chainHandlers = new ConcurrentHashSet<ChainHandler>();
	private int activeHandlers = 0;
	private int inactiveHandlers = 0;
	private int waitHandlers = 0;
	private int nChainHandlers = 0;
	private int chainCounter = 0;

	public static ChainHandlerManager getInstance() {
		return manager;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public void startChainHandlers(int nChainHandlers) {
		this.nChainHandlers = nChainHandlers;
		for (int j = 0; j < nChainHandlers; ++j) {
			log.debug("Starting Chain Handler " + j + " ...");
			ChainHandler handler = new ChainHandler(context);
			Thread thread = new Thread(handler);
			thread.setName("Chain Handler " + j);
			thread.start();
			chainHandlers.add(handler);
		}
		chainCounter += nChainHandlers;
	}

	public void startSeparateChainHandler(Chain chain) {
		ChainHandler handler = new ChainHandler(context, chain);
		ThreadPool.createNew(handler, "Separate Chain Handler");
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
		int singleChains = 0;
		ChainHandler[] handlers = chainHandlers
				.toArray(new ChainHandler[chainHandlers.size()]);
		ChainHandler firstActive = null;

		for (ChainHandler handler : handlers) {
			if (handler.getStatus() == ChainHandler.STATUS_FINISHED) {
				chainHandlers.remove(handler);
			} else if (handler.singleChain) {
				singleChains++;
			} else if (handler.getStatus() == ChainHandler.STATUS_INACTIVE) {
				inactiveHandlers++;
			} else if (handler.getStatus() == ChainHandler.STATUS_ACTIVE) {
				if (firstActive == null) {
					firstActive = handler;
				}
				activeHandlers++;
			} else if (handler.getStatus() == ChainHandler.STATUS_WAIT) {
				waitHandlers++;
			}
		}

		// Start a new chain if the number of non-blocked handlers is less than
		// the initial number of chain handlers.
		if (inactiveHandlers == 0 && activeHandlers < nChainHandlers
				&& chainsToProcess.getNElements() > 0) {
			ChainHandler handler = new ChainHandler(context);
			ThreadPool.createNew(handler, "Chain Handler " + chainCounter++);
			chainHandlers.add(handler);
		} else if (activeHandlers > nChainHandlers) {
			// signal one of the active handlers to stop when its chain is
			// finished.
			firstActive.stop();
		}
	}

	public void submissionFailed(int submissionId) {
		// Grab a lock on chainsToProcess, so that chain handlers can no longer
		// obtain chains.
		synchronized (chainsToProcess) {
			// Kill all chains that are waiting for a tuple iterator to become
			// ready.
			context.getChainNotifier().removeWaiters(submissionId);

			int nChains = chainsToProcess.getNElements();
			for (int i = 0; i < nChains; i++) {
				Chain ch = new Chain();
				chainsToProcess.remove(ch);
				if (ch.getSubmissionId() != submissionId) {
					chainsToProcess.add(ch);
				}
			}
			for (ChainHandler ch : chainHandlers) {
				ch.submissionFailed(submissionId);
			}
		}
	}
}
