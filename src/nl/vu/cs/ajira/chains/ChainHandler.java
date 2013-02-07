package nl.vu.cs.ajira.chains;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.statistics.StatisticsCollector;
import nl.vu.cs.ajira.storage.Container;
import nl.vu.cs.ajira.storage.container.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChainHandler implements Runnable {

	static final Logger log = LoggerFactory.getLogger(ChainHandler.class);

	private Context context = null;
	private NetworkLayer net = null;
	private Container<Chain> chainsToProcess = null;
	private ActionFactory ap = null;
	private StatisticsCollector stats = null;
	private boolean localMode;
	private Chain currentChain;

	private boolean submissionFailed;

	public ChainHandler(Context context) {
		this.context = context;
		this.net = context.getNetworkLayer();
		this.chainsToProcess = context.getChainsToProcess();
		this.stats = context.getStatisticsCollector();
		this.ap = context.getActionsProvider();
		localMode = context.isLocalMode();
	}
	
	private synchronized boolean getSubmissionFailed() {
		return submissionFailed;
	}
	
	public synchronized void submissionFailed(int submissionId) {
		if (currentChain != null && currentChain.getSubmissionId() == submissionId) {
			submissionFailed = true;
		}
	}

	@Override
	public void run() {

		currentChain = new Chain();
		Tuple tuple = new Tuple();
		WritableContainer<Chain> chainsBuffer = new WritableContainer<Chain>(
				Consts.SIZE_BUFFERS_CHILDREN_CHAIN_PROCESS);
		ChainExecutor actions = new ChainExecutor(context, chainsBuffer);

		while (true) {

			// Get a new chain to process
			try {
				chainsToProcess.remove(currentChain);
			} catch (Exception e) {
				log.error("Failed in retrieving a new chain."
						+ "This handler will be terminated", e);
				return;
			}
			
			// Init the environment
			actions.init(currentChain);

			try {
				currentChain.getActions(actions, ap);
			} catch(Throwable e) {
				// Broadcast all the nodes that a chain part of a job has
				// failed.
				log.error("getActions() on chain failed, cancelling the job ...", e);
				try {
					net.signalChainFailed(currentChain, e);
				} catch (Throwable e1) {
					log.error("Failed in managing to cancel the job", e);
				}
			}


			if (actions.getNActions() > 0) {

				// Read the input tuple from the knowledge base
				currentChain.getInputTuple(tuple);
				InputLayer input = context.getInputLayer(currentChain
						.getInputLayer());
				TupleIterator itr = input.getIterator(tuple, actions);
				if (!itr.isReady()) {
					context.getChainNotifier().addWaiter(itr, currentChain);
					currentChain = new Chain();
					continue;
				}

				submissionFailed = false;
				
				try {
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
								stats.addCounter(currentChain.getSubmissionNode(),
										currentChain.getSubmissionId(), counter,
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
							stats.addCounter(currentChain.getSubmissionNode(),
									currentChain.getSubmissionId(),
									"Chains Dynamically Generated",
									chainsBuffer.getNElements());
							if (localMode) {
								chainsToProcess.addAll(chainsBuffer);
							} else {
								net.sendChains(chainsBuffer);
							}
							chainsBuffer.clear();
						}
					} while (!eof && ! getSubmissionFailed());

					if (log.isDebugEnabled()) {
						timeCycle = System.currentTimeMillis() - timeCycle;
						log.debug("Chain " + currentChain.getChainId()
								+ "runtime cycle: " + timeCycle);
					}

					// Update eventual records
					if (nRecords > 0) {
						stats.addCounter(currentChain.getSubmissionNode(),
								currentChain.getSubmissionId(), counter, nRecords);
					}
					
					// Send the termination signal to the node responsible of
					// the submission
					if (actions.isChainFullyExecuted())
						net.signalChainTerminated(currentChain);
					stats.addCounter(currentChain.getSubmissionNode(),
							currentChain.getSubmissionId(), "Chains Processed", 1);

				} catch (Throwable e) {
					// Broadcast all the nodes that a chain part of a job has
					// failed.
					log.error("Chain failed. Cancelling the job ...", e);
					try {
						net.signalChainFailed(currentChain, e);
					} catch (Exception e1) {
						log.error("Failed in managing to cancel the job", e);
					}
					continue;
				} finally {
					input.releaseIterator(itr, actions);
				}
			}
		}
	}
}
