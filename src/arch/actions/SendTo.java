package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.buckets.Bucket;
import arch.buckets.Buckets;
import arch.chains.Chain;
import arch.data.types.SimpleData;
import arch.data.types.TByte;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.net.NetworkLayer;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class SendTo extends Action {

	public static final String THIS = "this";
	public static final String MULTIPLE = "multiple";
	public static final String ALL = "all";
	public static final String RANDOM = "random";

	static final Logger log = LoggerFactory.getLogger(SendTo.class);

	private static Random r = new Random();

	private int submissionNode;
	private int idSubmission;
	private long chainId;
	private long parentChainId;
	private int nchildren;
	private int replicatedFactor;
	private long responsibleChain;

	private int nodeId;
	private int bucket = -1;
	private boolean sc = true;
	private boolean ft = false;
	private String sortingFunction = null;
	private byte[] sortingParams = null;

	private final TInt tbucket = new TInt();
	private final TString tsorting = new TString();
	private final TInt tsub = new TInt();
	private final TInt tnode = new TInt();
	private final Tuple tuple = new Tuple();

	private NetworkLayer net = null;
	private Bucket[] bucketsCache;
	private Buckets buckets = null;
	private int totalNumberNodes = 0;

	@Override
	public void readFrom(DataInput input) throws IOException {
		nodeId = input.readInt();
		bucket = input.readInt();
		ft = input.readBoolean();
		sc = input.readBoolean();
		responsibleChain = input.readLong();
		int l = input.readByte();
		if (l > 0) {
			byte[] sSorting = new byte[l];
			input.readFully(sSorting);
			sortingFunction = new String(sSorting);
			l = input.readByte();
			if (l > 0) {
				sortingParams = new byte[l];
				for (int i = 0; i < l; ++i) {
					sortingParams[i] = input.readByte();
				}
			}
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(nodeId);
		output.writeInt(bucket);
		output.writeBoolean(ft);
		output.writeBoolean(sc);
		output.writeLong(responsibleChain);
		if (sortingFunction != null && sortingFunction.length() > 0) {
			byte[] raw = sortingFunction.getBytes();
			output.writeByte(raw.length);
			output.write(raw);
			if (sortingParams != null && sortingParams.length > 0) {
				output.writeByte(sortingParams.length);
				for (int i = 0; i < sortingParams.length; ++i) {
					output.writeByte(sortingParams[i]);
				}
			} else {
				output.writeByte(0);
			}
		} else {
			output.writeByte(0);
		}
	}

	@Override
	public int bytesToStore() {
		int size = 19;
		if (sortingFunction != null) {
			byte[] s = sortingFunction.getBytes();
			size += 1 + s.length;
			if (sortingParams != null) {
				size += sortingParams.length;
			}
		}
		return size;
	}

	public void setDestination(String destination) {
		if (destination.equals(THIS)) {
			nodeId = NetworkLayer.getInstance().getMyPartition();
		} else if (destination.equals(RANDOM)) {
			nodeId = r.nextInt(NetworkLayer.getInstance().getNumberNodes());
		} else if (destination.equals(MULTIPLE)) {
			nodeId = -1;
		} else if (destination.equals(ALL)) {
			nodeId = -2;
		} else {
			try {
				nodeId = Integer.valueOf(destination);
			} catch (Exception e) {
				log.warn("Unrecognized node (" + destination + ")! Set node=0");
				nodeId = 0;
			}
		}
	}

	public void setForwardTriples(boolean value) {
		ft = value;
	}

	public void setSendChain(boolean value) {
		sc = value;
	}

	public void setMainChainForBucket(long chain) {
		responsibleChain = chain;
	}

	public void setBucketId(int bucket) {
		this.bucket = bucket;
	}

	public void setSortingFunction(String sortingFunction, byte[] fields) {
		this.sortingFunction = sortingFunction;
		if (fields != null) {
			sortingParams = fields;
		}
	}

	public void setSortingFunction(String sortingFunction) {
		setSortingFunction(sortingFunction, null);
	}

	@Override
	public boolean blockProcessing() {
		return sc;
	}

	@Override
	public void startProcess(ActionContext context, Chain chain) {
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
	public void process(Tuple tuple, Chain remainingChain,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess,
			WritableContainer<Tuple> outputTuples, ActionContext context) {
		try {

			int nodeId = this.nodeId;
			if (nodeId == -1) {
				// Node to send is tuple's last record. Must remove it
				tuple.get(tnode, tuple.getNElements() - 1);
				nodeId = tnode.getValue();
				tuple.removeLast();
			}

			if (ft) {
				outputTuples.add(tuple);
			}

			if (nodeId >= 0) {
				Bucket b = bucketsCache[nodeId];
				if (b == null) {
					b = buckets.startTransfer(submissionNode, idSubmission,
							nodeId % totalNumberNodes, bucket, sortingFunction,
							sortingParams);
					bucketsCache[nodeId] = b;
				}
				b.add(tuple);
			} else { // Send it to all the nodes

				for (int i = 0; i < net.getNumberNodes(); ++i) {
					Bucket b = bucketsCache[i];
					if (b == null) {
						// Copy the tuple to the local buffer
						b = buckets.startTransfer(submissionNode, idSubmission,
								i, bucket, sortingFunction, sortingParams);
						bucketsCache[i] = b;
					}
					b.add(tuple);
				}

			}

		} catch (Exception e) {
			log.error(
					"Failed processing tuple. Chain="
							+ remainingChain.toString(), e);
		}
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> outputTuples,
			WritableContainer<Chain> newChains,
			WritableContainer<Chain> chainsToSend) {
		try {

			nchildren = chain.getChainChildren();
			replicatedFactor = chain.getReplicatedFactor();

			// Send the chains to process the buckets to all the nodes that
			// will host the buckets
			if (sc && chainId == responsibleChain && replicatedFactor > 0) {
				/*** AT FIRST SEND THE CHAINS ***/
				Chain newChain = new Chain();
				chain.copyTo(newChain);
				newChain.setExcludeExecution(false);
				newChain.setChainChildren(0);
				newChain.setReplicatedFactor(1);
				newChain.setInputLayerId(Consts.BUCKET_INPUT_LAYER_ID);
				tsub.setValue(idSubmission);
				tnode.setValue(nodeId);
				tbucket.setValue(bucket);
				if (sortingFunction != null) {
					tsorting.setValue(sortingFunction);
					if (sortingParams != null && sortingParams.length > 0) {
						SimpleData[] params = new SimpleData[4 + sortingParams.length];
						params[0] = tsub;
						params[1] = tbucket;
						params[2] = tsorting;
						for (int i = 0; i < sortingParams.length; ++i) {
							params[3 + i] = new TByte(sortingParams[i]);
						}
						params[params.length - 1] = tnode;
						this.tuple.set(params);
						for (int i = 0; i < sortingParams.length; ++i) {
							context.getDataProvider().release(params[3 + i]);
						}

						this.tuple.set(params);
					} else {
						this.tuple.set(tsub, tbucket, tsorting, tnode);
					}
				} else {
					this.tuple.set(tsub, tbucket, tnode);
				}
				newChain.replaceInputTuple(this.tuple);
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
						this.responsibleChain == this.chainId, sortingFunction,
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
