package nl.vu.cs.ajira.chains;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.mgmt.StatisticsCollector;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.storage.containers.WritableContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainHandler implements Runnable {

	static final Logger log = LoggerFactory.getLogger(ChainHandler.class);

	public static final int STATUS_INACTIVE = 0;
	public static final int STATUS_ACTIVE = 1;
	public static final int STATUS_WAIT = 2;
	public static final int STATUS_FINISHED = 3;

	private Context context = null;
	private NetworkLayer net = null;
	private WritableContainer<Chain> chainsToProcess = null;
	private ActionFactory ap = null;
	private StatisticsCollector stats = null;
	private Chain currentChain = new Chain();

	private boolean submissionFailed;
	private int status = STATUS_INACTIVE;
	public boolean singleChain = false;
	private Tuple tuple = TupleFactory.newTuple();
	private ChainExecutor actions;

	ChainHandler(Context context) {
		this.context = context;
		this.net = context.getNetworkLayer();
		this.chainsToProcess = context.getChainHandlerManager()
				.getChainsToProcess();
		this.stats = context.getStatisticsCollector();
		this.ap = context.getActionsProvider();
		actions = new ChainExecutor(this, context);
	}

	ChainHandler(Context context, Chain chain) {
		this(context);
		singleChain = true;
		chain.copyTo(this.currentChain);
	}

	private void processChain() {

		status = STATUS_ACTIVE;
		// Init the environment
		actions.init(currentChain);

		try {
			currentChain.getActions(actions, ap);
		} catch (Throwable e) {
			// Broadcast all the nodes that a chain part of a job has
			// failed.
			log.error("getActions() on chain failed, cancelling the job ...", e);
			try {
				net.signalChainFailed(currentChain, e);
			} catch (Throwable e1) {
				log.error("Failed in managing to cancel the job", e);
			}
			status = STATUS_INACTIVE;
			return;
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
				status = STATUS_INACTIVE;
				return;
			}

			if (log.isDebugEnabled()) {
				log.debug("Starting chain " + currentChain.getChainId());
			}

			submissionFailed = false;

			/***** START CHAIN *****/
			long timeCycle = System.currentTimeMillis();
			actions.setInputIterator(itr);
			try {
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
				} while (!eof && !getSubmissionFailed());

				if (log.isDebugEnabled()) {
					timeCycle = System.currentTimeMillis() - timeCycle;
					log.debug("Chain " + currentChain.getChainId()
							+ "runtime cycle: " + timeCycle);
				}

				// Send the termination signal to the node responsible of
				// the submission
				if (actions.isChainFullyExecuted()) {
					net.signalChainTerminated(currentChain);
				} else if (!actions.wasPrincipalBranch()) {
					int generatedChains = currentChain.getGeneratedRootChains();
					if (generatedChains > 0) {
						net.signalChainHasRootChains(currentChain,
								generatedChains);
					}
				}
				stats.addCounter(currentChain.getSubmissionNode(),
						currentChain.getSubmissionId(), "Chains Processed", 1);

			} catch (Throwable e) {
				// Broadcast all the nodes that a chain part of a job has
				// failed.
				log.error("Chain failed. Cancelling the job ...", e);
				try {
					net.signalChainFailed(currentChain, e);
				} catch (Exception e1) {
					log.error("Failed in managing to cancel the job.", e);
				}
			} finally {
				input.releaseIterator(itr, actions);
				status = STATUS_INACTIVE;
			}
		}
	}

	private synchronized boolean getSubmissionFailed() {
		return submissionFailed;
	}

	public synchronized void submissionFailed(int submissionId) {
		if (currentChain != null
				&& currentChain.getSubmissionId() == submissionId) {
			submissionFailed = true;
		}
	}

	@Override
	public void run() {

		if (singleChain) {
			if (log.isDebugEnabled()) {
				log.debug("Single chain handler");
			}
			processChain();
			if (log.isDebugEnabled()) {
				log.debug("Single chain handler done");
			}
			status = STATUS_FINISHED;
			return;
		}

		while (true) {
			// Get a new chain to process
			chainsToProcess.remove(currentChain);
			processChain();
		}
	}

	void setStatus(int status) {
		this.status = status;
	}

	public int getStatus() {
		return status;
	}
}
