package arch.net;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.WriteMessage;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.Context;
import arch.chains.Chain;

class ChainTerminator implements Runnable {

	public static class ChainInfo {

		public final int nodeId;
		public final int submissionId;
		public final long chainId;
		public final long parentChainId;
		// public int repFactor;
		public final int nchildren;
		public final boolean failed;
		public final Throwable exception;
		
		public ChainInfo(int nodeId, int submissionId, long chainId, long parentChainId, int nchildren) {
			this(nodeId, submissionId, chainId, parentChainId, nchildren, null);
		}

		public ChainInfo(int nodeId, int submissionId, long chainId,
				long parentChainId, int nchildren, Throwable exception) {
			this.nodeId = nodeId;
			this.submissionId = submissionId;
			this.chainId = chainId;
			this.parentChainId = parentChainId;
			this.nchildren = nchildren;
			if (exception != null) {
				failed = true;
				this.exception = exception;
			} else {
				failed = false;
				this.exception = null;
			}
		}
	}

	static final Logger log = LoggerFactory.getLogger(ChainTerminator.class);

	Context context;
	private final List<ChainInfo> chainsTerminated = Collections.synchronizedList(new LinkedList<ChainTerminator.ChainInfo>());

	public ChainTerminator(Context context) {
		this.context = context;
	}
	
	public void addInfo(ChainInfo ch) {
		synchronized(chainsTerminated) {
			chainsTerminated.add(ch);
			chainsTerminated.notify();
		}
	}
	
	public void addFailedChain(Chain chain, Throwable e) {
		ChainInfo ch = new ChainInfo(chain.getSubmissionNode(),
				chain.getSubmissionId(), chain.getChainId(),
				chain.getParentChainId(), chain.getTotalChainChildren(),
				e);
		addInfo(ch);
	}
	
	public void addChain(Chain chain) {
		ChainInfo ch = new ChainInfo(chain.getSubmissionNode(),
				chain.getSubmissionId(), chain.getChainId(),
				chain.getParentChainId(), chain.getTotalChainChildren());
		addInfo(ch);
	}

	@Override
	public void run() {
		ChainInfo header;
		NetworkLayer ibis = context.getNetworkLayer();

		boolean localMode = context.isLocalMode();

		while (true) {
			try {
				synchronized(chainsTerminated) {
					while (chainsTerminated.size() == 0) {
						chainsTerminated.wait();
					}
					header = chainsTerminated.remove(0);
				}

				if (!header.failed) {
					if (localMode) {
						context.getSubmissionsRegistry().updateCounters(
								header.submissionId, header.chainId,
								header.parentChainId, header.nchildren/*
																		 * ,
																		 * header
																		 * .
																		 * repFactor
																		 */);
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
						ibis.finishMessage(msg, header.submissionId);
						if (log.isDebugEnabled()) {
							log.debug("Sent message with id 2 to " + identifier);
						}
					}
				} else {
					// Broadcast to all the nodes that the submission ID
					// should be removed.
					if (localMode) {
						// FIXME:
						// context.cleanupSubmission(header.nodeId,
						// header.submissionId);
					} else {
						WriteMessage msg = ibis.getMessageToBroadcast();
						msg.writeByte((byte) 2);
						msg.writeBoolean(true);
						msg.writeInt(header.submissionId);
						msg.writeInt(header.nodeId);
						ibis.finishMessage(msg, header.submissionId);
					}
				}

			} catch (Exception e) {
				log.error("Error in sending the termination codes", e);
			}

		}
	}
}
