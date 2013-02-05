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

	private final Context context;
	private final Map<TupleIterator, Chain> waiters = new HashMap<TupleIterator, Chain>();
	private final ActionContext ac;

	public ChainNotifier(Context context) {
		this.context = context;
		ac = new ChainExecutor(context, context.isLocalMode());
	}

	public synchronized void addWaiter(TupleIterator iter, Chain chain) {
		waiters.put(iter, chain);
		iter.registerReadyNotifier(this);
		if (log.isInfoEnabled()) {
			log.info("Add waiter " + iter + "; size is now " + waiters.size());
		}
	}

	public void markReady(TupleIterator iter) {
		Chain chain;
		if (log.isInfoEnabled()) {
			log.info("Mark ready for iter " + iter);
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
			context.getChainsToProcess().add(chain);
		} catch (Exception e) {
			log.error("Error in adding the chain", e);
		}
	}
}
