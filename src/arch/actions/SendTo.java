package arch.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.buckets.Bucket;
import arch.buckets.Buckets;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.net.NetworkLayer;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class SendTo extends Action {

	public static final int MULTIPLE = -1;
	public static final int ALL = -2;

	/* PARAMETERS */
	public static final int NODE_ID = 0;
	public static final String S_NODE_ID = "node_id";
	public static final int BUCKET_ID = 1;
	public static final String S_BUCKET_ID = "bucket_id";
	public static final int FORWARD_TUPLES = 2;
	public static final String S_FORWARD_TUPLES = "forward_tuples";
	public static final int SEND_CHAIN = 3;
	public static final String S_SEND_CHAIN = "send_chain";
	public static final int SORTING_FUNCTION = 4;
	public static final String S_SORTING_FUNCTION = "sorting_function";

	static final Logger log = LoggerFactory.getLogger(SendTo.class);

	private int submissionNode;
	private int idSubmission;
	private long chainId;
	private long parentChainId;
	private int nchildren;
	private int replicatedFactor;

	private int nodeId;
	private int bucket = -1;
	private boolean sc = true;
	private boolean ft = false;
	private String sortingFunction = null;
	private byte[] sortingParams = null;

	private final TInt tbucket = new TInt();
	private final TInt tsub = new TInt();
	private final TInt tnode = new TInt();
	private final Tuple tuple = new Tuple();

	private NetworkLayer net = null;
	private Bucket[] bucketsCache;
	private Buckets buckets = null;
	private int totalNumberNodes = 0;

	@Override
	public boolean blockProcessing() {
		return sc;
	}

	@Override
	public void setupActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(NODE_ID, S_NODE_ID, null, true);
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
		conf.registerParameter(FORWARD_TUPLES, S_FORWARD_TUPLES, false, false);
		conf.registerParameter(SEND_CHAIN, S_SEND_CHAIN, true, false);
		conf.registerParameter(SORTING_FUNCTION, S_SORTING_FUNCTION, null,
				false);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		nodeId = getParamInt(NODE_ID);
		bucket = getParamInt(BUCKET_ID);
		ft = getParamBoolean(FORWARD_TUPLES);
		sc = getParamBoolean(SEND_CHAIN);
		sortingFunction = getParamString(SORTING_FUNCTION);

		// Init variables
		net = context.getNetworkLayer();
		bucketsCache = new Bucket[net.getNumberNodes()];
		submissionNode = chain.getSubmissionNode();
		idSubmission = chain.getSubmissionId();
		chainId = chain.getChainId();
		parentChainId = chain.getParentChainId();
		buckets = context.getTuplesBuckets();
		totalNumberNodes = NetworkLayer.getInstance().getNumberNodes();
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> outputTuples,
			WritableContainer<Chain> chainsToProcess) {
		try {
			int nodeId = this.nodeId;
			if (nodeId == -1) {
				// Node to send is tuple's last record. Must remove it
				inputTuple.get(tnode, inputTuple.getNElements() - 1);
				nodeId = tnode.getValue();
				inputTuple.removeLast();
			}

			if (nodeId >= 0) {
				Bucket b = bucketsCache[nodeId];
				if (b == null) {
					b = buckets.startTransfer(submissionNode, idSubmission,
							nodeId % totalNumberNodes, bucket, sortingFunction,
							sortingParams);
					bucketsCache[nodeId] = b;
				}
				b.add(inputTuple);
			} else { // Send it to all the nodes
				for (int i = 0; i < net.getNumberNodes(); ++i) {
					Bucket b = bucketsCache[i];
					if (b == null) {
						// Copy the tuple to the local buffer
						b = buckets.startTransfer(submissionNode, idSubmission,
								i, bucket, sortingFunction, sortingParams);
						bucketsCache[i] = b;
					}
					b.add(inputTuple);
				}
			}

			if (ft) {
				outputTuples.add(inputTuple);
			}

		} catch (Exception e) {
			log.error("Failed processing tuple. Chain=" + chain.toString(), e);
		}
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> outputTuples,
			WritableContainer<Chain> chainsToSend) {
		try {
			nchildren = chain.getChainChildren();
			replicatedFactor = chain.getReplicatedFactor();

			// Send the chains to process the buckets to all the nodes that
			// will host the buckets
			if (sc && context.isCurrentChainRoot() && replicatedFactor > 0) {
				/*** AT FIRST SEND THE CHAINS ***/
				Chain newChain = new Chain();
				chain.copyTo(newChain);
				newChain.setChainChildren(0);
				newChain.setReplicatedFactor(1);
				newChain.setInputLayerId(Consts.BUCKET_INPUT_LAYER_ID);
				tsub.setValue(idSubmission);
				tnode.setValue(nodeId);
				tbucket.setValue(bucket);
				this.tuple.set(tsub, tbucket, tnode);
				newChain.setInputTuple(this.tuple);
				chainsToSend.add(newChain);
			}

			int startNode, endNode;
			if (nodeId == -1 || nodeId == -2) {
				startNode = 0;
				endNode = net.getNumberNodes();
			} else {
				startNode = nodeId;
				endNode = nodeId + 1;
			}

			if (log.isDebugEnabled()) {
				log.debug("SendTo.stopProcess: SendTo = " + this
						+ ", startNode = " + startNode + ", endNode = "
						+ endNode + ", bucketID = "
						+ Buckets.getKey(idSubmission, bucket) + ", chainID = "
						+ chainId);
			}

			while (startNode < endNode) {
				buckets.finishTransfer(submissionNode, idSubmission, startNode,
						this.bucket, this.chainId, this.parentChainId,
						this.nchildren, this.replicatedFactor,
						context.isCurrentChainRoot(), sortingFunction,
						sortingParams, bucketsCache[startNode] != null);
				++startNode;
			}
		} catch (Exception e) {
			log.error("Error", e);
		}
		bucketsCache = null;
		net = null;
		buckets = null;
	}
}
