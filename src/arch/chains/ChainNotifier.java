package arch.chains;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.Context;
import arch.actions.ActionContext;
import arch.datalayer.InputLayer;
import arch.datalayer.TupleIterator;

public class ChainNotifier {

	static final Logger log = LoggerFactory.getLogger(ChainNotifier.class);

	private final Context context;
	private final Map<TupleIterator, Chain> waiters = new HashMap<TupleIterator, Chain>();
	private final ActionContext ac;

	public ChainNotifier(Context context) {
		this.context = context;
		ac = new ActionsExecutor(context, null);
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
