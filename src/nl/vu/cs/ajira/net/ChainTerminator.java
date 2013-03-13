package nl.vu.cs.ajira.net;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.WriteMessage;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.chains.Chain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ChainTerminator implements Runnable {

	public final static class ChainInfo {

		public final int nodeId;
		public final int submissionId;
		public final long chainId;
		public final long parentChainId;
		public final int nchildren;
		public final boolean failed;
		public final Throwable exception;
		public final long[] additionalChainCounters;
		public final int[] additionalChainValues;

		public ChainInfo(int nodeId, int submissionId, long chainId,
				long parentChainId, int nchildren,
				long[] additionalChainCounters, int[] additionalChainValues) {
			this(nodeId, submissionId, chainId, parentChainId, nchildren,
					additionalChainCounters, additionalChainValues, null);
		}

		public ChainInfo(int nodeId, int submissionId, long chainId,
				long parentChainId, int nchildren,
				long[] additionalChainCounters, int[] additionalChainValues,
				Throwable exception) {
			this.nodeId = nodeId;
			this.submissionId = submissionId;
			this.chainId = chainId;
			this.parentChainId = parentChainId;
			this.nchildren = nchildren;
			this.additionalChainCounters = additionalChainCounters;
			this.additionalChainValues = additionalChainValues;
			if (exception != null) {
				failed = true;
				this.exception = exception;
			} else {
				failed = false;
				this.exception = null;
			}
		}

		public ChainInfo(int nodeId, int submissionId) {
			this.nodeId = nodeId;
			this.submissionId = submissionId;
			this.chainId = -1;
			this.parentChainId = -2;
			this.failed = false;
			this.exception = null;
			this.nchildren = 0;
			this.additionalChainCounters = null;
			this.additionalChainValues = null;
		}
	}

	static final Logger log = LoggerFactory.getLogger(ChainTerminator.class);

	Context context;
	private final List<ChainInfo> chainsTerminated = Collections
			.synchronizedList(new LinkedList<ChainTerminator.ChainInfo>());

	public ChainTerminator(Context context) {
		this.context = context;
	}

	public void addInfo(ChainInfo ch) {
		synchronized (chainsTerminated) {
			chainsTerminated.add(ch);
			chainsTerminated.notify();
		}
	}

	public void addFailedChain(Chain chain, Throwable e) {
		ChainInfo ch = new ChainInfo(chain.getSubmissionNode(),
				chain.getSubmissionId(), chain.getChainId(),
				chain.getParentChainId(), chain.getTotalChainChildren(), null,
				null, e);
		addInfo(ch);
	}

	public void addChain(Chain chain,
			Map<Long, List<Integer>> additionalCounters) {

		ChainInfo ch = null;
		if (additionalCounters != null && additionalCounters.size() > 0) {
			long[] chains = new long[additionalCounters.size()];
			int[] values = new int[additionalCounters.size()];
			int i = 0;
			for (Map.Entry<Long, List<Integer>> entry : additionalCounters
					.entrySet()) {
				chains[i] = entry.getKey();
				values[i] = entry.getValue().size();
			}
			ch = new ChainInfo(chain.getSubmissionNode(),
					chain.getSubmissionId(), chain.getChainId(),
					chain.getParentChainId(), chain.getTotalChainChildren(),
					chains, values);
		} else {
			ch = new ChainInfo(chain.getSubmissionNode(),
					chain.getSubmissionId(), chain.getChainId(),
					chain.getParentChainId(), chain.getTotalChainChildren(),
					null, null);
		}
		addInfo(ch);
	}

	@Override
	public void run() {
		ChainInfo header;
		NetworkLayer ibis = context.getNetworkLayer();

		boolean localMode = context.isLocalMode();

		while (true) {
			try {
				synchronized (chainsTerminated) {
					while (chainsTerminated.size() == 0) {
						chainsTerminated.wait();
					}
					header = chainsTerminated.remove(0);
				}

				if (!header.failed) {
					if (localMode) {
						context.getSubmissionsRegistry().updateCounters(
								header.submissionId, header.chainId,
								header.parentChainId, header.nchildren,
								header.additionalChainCounters,
								header.additionalChainValues);
					} else {
						IbisIdentifier identifier = ibis
								.getPeerLocation(header.nodeId);
						WriteMessage msg = ibis.getMessageToSend(identifier,
								NetworkLayer.nameMgmtReceiverPort);
						msg.writeByte((byte) 2);
						msg.writeBoolean(false);
						msg.writeInt(header.submissionId);
						msg.writeLong(header.chainId);
						msg.writeLong(header.parentChainId);
						msg.writeInt(header.nchildren);

						if (header.additionalChainCounters != null) {
							msg.writeInt(header.additionalChainCounters.length);
							for (int i = 0; i < header.additionalChainCounters.length; ++i) {
								msg.writeLong(header.additionalChainCounters[i]);
								msg.writeInt(header.additionalChainValues[i]);
							}
						} else {
							msg.writeInt(0);
						}

						ibis.finishMessage(msg, header.submissionId);
						if (log.isDebugEnabled()) {
							log.debug("Sent message with id 2 to " + identifier);
						}
					}
				} else {
					// Broadcast to all the nodes that the submission ID
					// should be removed.
					if (localMode) {
						context.cleanupSubmission(header.nodeId,
								header.submissionId, header.exception);
					} else {
						WriteMessage msg = ibis.getMessageToBroadcast();
						msg.writeByte((byte) 2);
						msg.writeBoolean(true);
						msg.writeInt(header.submissionId);
						msg.writeInt(header.nodeId);
						msg.writeObject(header.exception);
						ibis.finishMessage(msg, header.submissionId);
					}
				}

			} catch (Exception e) {
				log.error("Error in sending the termination codes", e);
			}

		}
	}
}
