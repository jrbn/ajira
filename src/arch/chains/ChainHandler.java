package arch.chains;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.StatisticsCollector;
import arch.actions.Action;
import arch.actions.ActionsProvider;
import arch.data.types.DataProvider;
import arch.data.types.Tuple;
import arch.data.types.bytearray.FDataInput;
import arch.datalayer.InputLayer;
import arch.datalayer.TupleIterator;
import arch.net.NetworkLayer;
import arch.storage.Container;
import arch.storage.Factory;
import arch.storage.RawComparator;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class ChainHandler extends WritableContainer<Tuple> implements
		ChainContinuation, Runnable {

	static final Logger log = LoggerFactory.getLogger(ChainHandler.class);

	NetworkLayer net = null;
	Container<Chain> chainsToProcess = null;
	Context context = null;
	ActionsProvider ap = null;
	DataProvider dp = null;
	StatisticsCollector stats = null;
	WritableContainer<Chain> chainsBuffer = new WritableContainer<Chain>(
			Consts.SIZE_BUFFERS_CHILDREN_CHAIN_PROCESS);
	WritableContainer<Chain> chainsBuffer2 = new WritableContainer<Chain>(
			Consts.SIZE_BUFFERS_CHILDREN_CHAIN_PROCESS);

	Chain chain = new Chain();
	Tuple tuple = new Tuple();

	String[] actionNames = new String[Consts.MAX_N_ACTIONS];
	int[] rawSizes = new int[Consts.MAX_N_ACTIONS];
	Action[] actions = new Action[Consts.MAX_N_ACTIONS];
	Object[][] params = new Object[Consts.MAX_N_ACTIONS][Consts.MAX_N_PARAMS];

	ActionContext ac;
	long chainIDCounter;
	int bucketIDCounter;
	int numberActions;
	int indexAction;
	boolean blockProcessing;

	public boolean active;

	public ChainHandler(Context context) {
		super(0);
		this.context = context;
		this.net = context.getNetworkLayer();
		this.chainsToProcess = context.getChainsToProcess();
		this.stats = context.getStatisticsCollector();
		this.ap = context.getActionsProvider();
		this.dp = context.getDataProvider();
		ac = new ActionContext(context, dp);
		try {
			chainIDCounter = (net.getCounter("chainID") + 1) << 40;
			bucketIDCounter = ((int) net.getCounter("bucketID") + 1) << 16;
		} catch (Throwable e) {
			log.error("Error in initializing chain handler", e);
		}
		ac.setStartingChainID(chainIDCounter);
		ac.setStartingBucketID(bucketIDCounter);
	}

	@Override
	public int compare(byte[] buffer, int start) {
		throw new Error("Not allowed");
	}

	@Override
	public int bytesToStore() {
		throw new Error("Not allowed");
	}

	@Override
	public int getRawElementsSize() {
		throw new Error("Not allowed");
	}

	@Override
	public void clear() {
		throw new Error("Not allowed");
	}

	@Override
	public boolean addAll(WritableContainer<Tuple> buffer) throws Exception {
		throw new Error("Not allowed");
	}

	@Override
	public byte[] removeRaw(byte[] value) throws Exception {
		throw new Error("Not allowed");
	}

	@Override
	public int getNElements() {
		throw new Error("Not allowed");
	}

	@Override
	public void copyTo(WritableContainer<?> buffer) {
		throw new Error("Not allowed");
	}

	@Override
	public boolean addRaw(byte[] key) throws IOException {
		throw new Error("Not allowed");
	}

	@Override
	public int compareTo(WritableContainer<Tuple> buffer) {
		throw new Error("Not allowed");
	}

	@Override
	public void addRaw(WritableContainer<Tuple> buffer, int i) throws Exception {
		throw new Error("Not allowed");
	}

	@Override
	public boolean addAll(FDataInput originalStream, byte[] lastEl,
			long nElements, long size) throws Exception {
		throw new Error("Not allowed");
	}

	@Override
	public boolean equals(Object obj) {
		throw new Error("Not allowed");
	}

	@Override
	public boolean get(Tuple element) throws Exception {
		throw new Error("Not allowed");
	}

	@Override
	public boolean get(Tuple element, int index) throws Exception {
		throw new Error("Not allowed");
	}

	@Override
	public int getHash(int maxBytes) {
		throw new Error("Not allowed");
	}

	@Override
	public int getHash(int index, int maxBytes) throws Exception {
		throw new Error("Not allowed");
	}

	@Override
	public int hashCode() {
		throw new Error("Not allowed");
	}

	@Override
	public int remainingCapacity(int maxSize) {
		throw new Error("Not allowed");
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		throw new Error("Not allowed");
	}

	@Override
	public boolean remove(Tuple element) throws Exception {
		throw new Error("Not allowed");
	}

	@Override
	public void moveTo(WritableContainer<?> buffer) {
		throw new Error("Not allowed");
	}

	@Override
	public void removeLast() throws Exception {
		throw new Error("Not allowed");
	}

	@Override
	public void sort(RawComparator<Tuple> c,
			Factory<WritableContainer<Tuple>> fb) throws IOException {
		throw new Error("Not allowed");
	}

	@Override
	public byte[] returnLastElement() throws IOException {
		throw new Error("Not allowed");
	}

	@Override
	public String toString() {
		throw new Error("Not allowed");
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		throw new Error("Not allowed");
	}

	@Override
	public void writeElementsTo(DataOutput cacheOutputStream)
			throws IOException {
		throw new Error("Not allowed");
	}

	@Override
	public boolean add(Tuple element) throws Exception {
		if (indexAction + 1 < numberActions
				&& !actions[indexAction].blockProcessing()) {
			indexAction++;
			chain.setRawSize(rawSizes[indexAction]);
			actions[indexAction].process(ac, chain, element, this,
					chainsBuffer2);
			indexAction--;
		}
		return true;
	}

	@Override
	public void run() {
		while (true) {

			// Get a new chain to process
			try {
				active = false;
				chainsToProcess.remove(chain);
				active = true;
			} catch (Exception e) {
				log.error("Failed in retrieving a new chain."
						+ "This handler will be terminated", e);
				return;
			}

			try {
				ac.setCurrentChain(chain);

				// Start the process
				numberActions = chain.getActions(actions, rawSizes, ap);
				blockProcessing = false;

				if (numberActions != 0) {

					// Read the input tuple from the knowledge base
					chain.getInputTuple(tuple);
					InputLayer input = context.getInputLayer(chain
							.getInputLayerId());
					TupleIterator itr = input.getIterator(tuple, ac);
					if (!itr.isReady()) {
						context.getChainNotifier().addWaiter(itr, chain);
						chain = new Chain();
						continue;
					}

					/***** START CHAIN *****/
					long timeCycle = System.currentTimeMillis();
					int sizeChain = chain.getRawSize();
					for (int i = 0; i < numberActions && !blockProcessing; i++) {
						chain.setRawSize(rawSizes[i]);
						actions[i].startProcess(ac, chain);
						blockProcessing = actions[i].blockProcessing();
					}
					chain.setRawSize(sizeChain);
					String counter = "Records input " + chain.getInputLayerId();

					// Process the data on the chain
					boolean eof = false;
					long nRecords = 0;

					do {
						// Init
						chainsBuffer.clear();
						chainsBuffer2.clear();

						eof = !itr.next() || chain.getExcludeExecution();
						if (!eof) {
							nRecords++;
							if (nRecords == 10000) {
								stats.addCounter(chain.getSubmissionNode(),
										chain.getSubmissionId(), counter,
										nRecords);
								nRecords = 0;
							}

							itr.getTuple(tuple);

							chain.setRawSize(rawSizes[0]);

							indexAction = 0;
							actions[indexAction].process(ac, chain, tuple,
									this, chainsBuffer2);

						} else { // EOF Case
							indexAction = 0;
							while (indexAction < numberActions) {
								chain.setRawSize(rawSizes[indexAction]);

								actions[indexAction].stopProcess(ac, chain,
										this, chainsBuffer2);
								if (actions[indexAction].blockProcessing()) {
									ap.release(actions[indexAction]);
									break;
								} else {
									ap.release(actions[indexAction]);
									indexAction++;
								}
							}
						}

						// Update the children generated in this action
						if (chainsBuffer.getNElements() > 0) {
							stats.addCounter(chain.getSubmissionNode(),
									chain.getSubmissionId(),
									"Chains Generated From Chains",
									chainsBuffer.getNElements());
							chainsBuffer.clear();
						}

						// Update the children generated in this action
						if (chainsBuffer2.getNElements() > 0) {
							stats.addCounter(
									chain.getSubmissionNode(),
									chain.getSubmissionId(),
									"Chains Generated From Chains (To Process)",
									chainsBuffer2.getNElements());
							net.sendChains(chainsBuffer2);
							chainsBuffer2.clear();
						}

						chain.setRawSize(sizeChain);
					} while (!eof);

					if (log.isDebugEnabled()) {
						timeCycle = System.currentTimeMillis() - timeCycle;
						log.debug("Chain " + chain.getChainId()
								+ "runtime cycle: " + timeCycle);
					}

					input.releaseIterator(itr, ac);

					// Update eventual records
					if (nRecords > 0) {
						stats.addCounter(chain.getSubmissionNode(),
								chain.getSubmissionId(), counter, nRecords);
					}
				}

				// Send the termination signal to the node responsible of
				// the
				// submission
				if (!blockProcessing) {
					net.signalChainTerminated(chain);
				}

				stats.addCounter(chain.getSubmissionNode(),
						chain.getSubmissionId(), "Chains Processed", 1);

			} catch (Exception e) {
				// Broadcast all the nodes that a chain part of a job has
				// failed.
				log.error("Chain is failed. Cancelling the job ...", e);
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
