package arch.chains;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.Context;
import arch.actions.ActionFactory;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.datalayer.TupleIterator;
import arch.net.NetworkLayer;
import arch.statistics.StatisticsCollector;
import arch.storage.Container;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class ChainHandler implements Runnable {

	static final Logger log = LoggerFactory.getLogger(ChainHandler.class);

	private Context context = null;
	private NetworkLayer net = null;
	private Container<Chain> chainsToProcess = null;
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
		Tuple tuple = new Tuple();
		WritableContainer<Chain> chainsBuffer = new WritableContainer<Chain>(
				Consts.SIZE_BUFFERS_CHILDREN_CHAIN_PROCESS);
		ActionsExecutor actions = new ActionsExecutor(context, chainsBuffer);

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
					actions.startProcess();
					String counter = "Records Read From Input "
							+ input.getName();

					// Process the data on the chain
					boolean eof = false;
					long nRecords = 0;

					do {
						eof = !itr.next();
						if (!eof) {
							nRecords++;
							if (nRecords == 10000) {
								stats.addCounter(chain.getSubmissionNode(),
										chain.getSubmissionId(), counter,
										nRecords);
								nRecords = 0;
							}

							itr.getTuple(tuple);
							actions.output(tuple);

						} else { // EOF Case
							actions.stopProcess();
						}

						// Update the children generated in this action
						if (chainsBuffer.getNElements() > 0) {
							stats.addCounter(chain.getSubmissionNode(),
									chain.getSubmissionId(),
									"ChainHandler:run: chains generated from chains",
									chainsBuffer.getNElements());
							if (localMode) {
								chainsToProcess.addAll(chainsBuffer);
							} else {
								net.sendChains(chainsBuffer);
							}
							chainsBuffer.clear();
						}
					} while (!eof);

					if (log.isDebugEnabled()) {
						timeCycle = System.currentTimeMillis() - timeCycle;
						log.debug("Chain " + chain.getChainId()
								+ " runtime cycle: " + timeCycle);
					}

					input.releaseIterator(itr, actions);

					// Update eventual records
					if (nRecords > 0) {
						stats.addCounter(chain.getSubmissionNode(),
								chain.getSubmissionId(), counter, nRecords);
					}
				}

				// Send the termination signal to the node responsible of
				// the submission
				if (actions.isChainFullyExecuted())
					net.signalChainTerminated(chain);
				stats.addCounter(chain.getSubmissionNode(),
						chain.getSubmissionId(), "ChainHandler:run: chains processed", 1);

			} catch (Exception e) {
				// Broadcast all the nodes that a chain part of a job has
				// failed.
				log.error("Chain failed. Cancelling the job ...", e);
				try {

					context.cleanupSubmission(chain.getSubmissionNode(),
							chain.getSubmissionId());
					net.signalChainFailed(chain);
				} catch (Exception e1) {
					log.error("Failed in managing to cancel the job."
							+ "This instance will be terminated.", e);
					System.exit(1);
				}
			}
		}
	}
}
