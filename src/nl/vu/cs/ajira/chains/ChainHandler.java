package nl.vu.cs.ajira.chains;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.statistics.StatisticsCollector;
import nl.vu.cs.ajira.storage.containers.WritableContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainHandler implements Runnable {

	static final Logger log = LoggerFactory.getLogger(ChainHandler.class);

	private Context context = null;
	private NetworkLayer net = null;
	private WritableContainer<Chain> chainsToProcess = null;
	private ActionFactory ap = null;
	private StatisticsCollector stats = null;
	private boolean localMode;

	public ChainHandler(Context context) {
		this.context = context;
		this.net = context.getNetworkLayer();
		this.chainsToProcess = context.getChainsToProcess();
		this.stats = context.getStatisticsCollector();
		this.ap = context.getActionsProvider();
		localMode = context.isLocalMode();
	}

	@Override
	public void run() {

		Chain chain = new Chain();
		Tuple tuple = TupleFactory.newTuple();

		ChainExecutor actions = new ChainExecutor(context, chainsToProcess);

		// if (localMode) {
		// actions = new ChainExecutor(context, chainsToProcess);
		// } else {
		// // net.sendChains(chainsBuffer); //FIXME
		// }

		while (true) {

			// Get a new chain to process
			try {
				chainsToProcess.remove(chain);
			} catch (Exception e) {
				log.error("Failed in retrieving a new chain."
						+ "This handler will be terminated", e);
				return;
			}

			try {
				// Init the environment
				actions.init(chain);
				chain.getActions(actions, ap);

				if (actions.getNActions() > 0) {

					// Read the input tuple from the knowledge base
					chain.getInputTuple(tuple);
					InputLayer input = context.getInputLayer(chain
							.getInputLayer());
					TupleIterator itr = input.getIterator(tuple, actions);
					if (!itr.isReady()) {
						context.getChainNotifier().addWaiter(itr, chain);
						chain = new Chain();
						continue;
					}

					/***** START CHAIN *****/
					long timeCycle = System.currentTimeMillis();
					actions.setInputIterator(itr);
					actions.startProcess();

					// Process the data on the chain
					boolean eof = false;
					do {
						eof = !itr.nextTuple();
						if (!eof) {
							itr.getTuple(tuple);
							actions.output(tuple);
						} else { // EOF Case
							actions.stopProcess();
						}
					} while (!eof);

					if (log.isDebugEnabled()) {
						timeCycle = System.currentTimeMillis() - timeCycle;
						log.debug("Chain " + chain.getChainId()
								+ "runtime cycle: " + timeCycle);
					}

					input.releaseIterator(itr, actions);
				}

				// Send the termination signal to the node responsible of
				// the submission
				if (actions.isChainFullyExecuted())
					net.signalChainTerminated(chain);
				stats.addCounter(chain.getSubmissionNode(),
						chain.getSubmissionId(), "Chains Processed", 1);

			} catch (Throwable e) {
				// Broadcast all the nodes that a chain part of a job has
				// failed.
				log.error("Chain failed. Cancelling the job ...", e);
				try {
					net.signalChainFailed(chain, e);
				} catch (Exception e1) {
					log.error("Failed in managing to cancel the job."
							+ "This instance will be terminated.", e);
					System.exit(1);
				}
			}
		}
	}
}
