package nl.vu.cs.ajira.chains;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainNotifier {

	static final Logger log = LoggerFactory.getLogger(ChainNotifier.class);

	private Context context;
	private final Map<TupleIterator, Chain> waiters = new HashMap<TupleIterator, Chain>();
	private ActionContext ac;

	public ChainNotifier() {
	}

	public void init(Context context) {
		this.context = context;
		ac = new ChainExecutor(null, context);
	}

	/**
	 * Removes all waiters from a specific submission id. To be used when a
	 * submission fails for some reason.
	 * 
	 * @param submissionId
	 *            the submission that failed.
	 */
	public synchronized void removeWaiters(int submissionId) {
		TupleIterator[] w = waiters.keySet().toArray(new TupleIterator[0]);
		for (TupleIterator i : w) {
			Chain ch = waiters.get(i);
			if (ch != null && ch.getSubmissionId() == submissionId) {
				waiters.remove(i);
				InputLayer input = context.getInputLayer(ch.getInputLayer());
				input.releaseIterator(i, ac);
			}
		}
	}

	public synchronized void addWaiter(TupleIterator iter, Chain chain) {
		waiters.put(iter, chain);
		iter.registerReadyNotifier(this);
		if (log.isDebugEnabled()) {
			log.debug("Add waiter " + iter + "; size is now " + waiters.size());
		}
	}

	public void markReady(TupleIterator iter) {
		Chain chain;
		if (log.isDebugEnabled()) {
			log.debug("Mark ready for iter " + iter);
		}
		synchronized (this) {
			chain = waiters.get(iter);
			if (chain == null) {
				if (log.isInfoEnabled()) {
					log.info("No chain!");
				}
				return;
			}
			waiters.remove(iter);
		}
		InputLayer input = context.getInputLayer(chain.getInputLayer());
		input.releaseIterator(iter, ac);
		try {
			context.getChainHandlerManager().getChainsToProcess().add(chain);
		} catch (Exception e) {
			log.error("Error in adding the chain", e);
		}
	}
}
