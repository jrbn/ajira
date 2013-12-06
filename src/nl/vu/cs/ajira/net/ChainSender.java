package nl.vu.cs.ajira.net;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.WriteMessage;

import java.io.IOException;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.chains.Chain;
import nl.vu.cs.ajira.chains.ChainExecutor;
import nl.vu.cs.ajira.chains.Location;
import nl.vu.cs.ajira.mgmt.StatisticsCollector;
import nl.vu.cs.ajira.storage.Container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class is used to send chains to other nodes.
 * 
 */
class ChainSender implements Runnable {

	static final Logger log = LoggerFactory.getLogger(ChainSender.class);

	Container<Chain> chainsToSend;
	Container<Chain> chainsToProcess;
	Context context;
	Chain chain;
	StatisticsCollector stats;

	/**
	 * Custom constructor.
	 * 
	 * @param context
	 *            Current context.
	 * @param chainsToSend
	 *            The chains that have to be send.
	 */
	public ChainSender(Context context, Container<Chain> chainsToSend) {
		this.chainsToSend = chainsToSend;
		this.context = context;
		this.chainsToProcess = context.getChainHandlerManager()
				.getChainsToProcess();
		this.stats = context.getStatisticsCollector();
	}

	@Override
	public void run() {
		Query query = new Query();
		Chain chain = new Chain();
		Chain supportChain = new Chain();

		while (true) {

			WriteMessage msg = null;

			try {
				chainsToSend.remove(chain);
				ChainExecutor ac = new ChainExecutor(null, context, chain);

				chain.getQuery(query);
				Location loc = context.getInputLayer(chain.getInputLayer())
						.getLocations(query.getTuple(), ac);

				NetworkLayer ibis = context.getNetworkLayer();
				IbisIdentifier[] nodes = ibis.getPeersLocation(loc);

				if (nodes.length == 0) { // Put it directly in the queue
					chainsToProcess.add(chain);
				} else { // Send the chains

					int i = nodes.length - 1;
					while (i != 0) {
						chain.branch(supportChain, context
								.getChainCounter(chain.getSubmissionId()), 0);

						if (nodes[i].compareTo(ibis.ibis.identifier()) == 0) {
							chainsToProcess.add(supportChain);
						} else {
							msg = ibis.getMessageToSend(nodes[i],
									NetworkLayer.nameMgmtReceiverPort);
							msg.writeByte((byte) 0);
							supportChain.writeTo(new WriteMessageWrapper(msg));
							ibis.finishMessage(msg,
									supportChain.getSubmissionId());
							msg = null;
						}
						i--;
					}

					if (nodes[0].compareTo(ibis.ibis.identifier()) == 0) {
						chainsToProcess.add(chain);
					} else {
						msg = ibis.getMessageToSend(nodes[0],
								NetworkLayer.nameMgmtReceiverPort);
						msg.writeByte((byte) 0);
						chain.writeTo(new WriteMessageWrapper(msg));
						ibis.finishMessage(msg, chain.getSubmissionId());
						msg = null;
					}
				}

			} catch (Throwable e) {
				if (msg != null && e instanceof IOException) {
					msg.finish((IOException) e);
				}
				if (log.isDebugEnabled()) {
					log.debug(
							"Error in the main execution of the communicator thread",
							e);
				}
				context.killSubmission(chain.getSubmissionNode(),
						chain.getSubmissionId(), e);
			}
		}
	}
}
