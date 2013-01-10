package arch.net;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.WriteMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.Context;
import arch.actions.ActionContext;
import arch.chains.ActionsExecutor;
import arch.chains.Chain;
import arch.chains.ChainLocation;
import arch.data.types.Tuple;
import arch.statistics.StatisticsCollector;
import arch.storage.Container;
import arch.utils.Consts;

class ChainSender implements Runnable {

	static final Logger log = LoggerFactory.getLogger(ChainSender.class);

	Container<Chain> chainsToSend;
	Container<Chain> chainsToProcess;
	Context context;
	Chain chain;
	StatisticsCollector stats;

	public ChainSender(Context context, Container<Chain> chainsToSend) {
		this.chainsToSend = chainsToSend;
		this.context = context;
		this.chainsToProcess = context.getChainsToProcess();
		this.stats = context.getStatisticsCollector();
	}

	@Override
	public void run() {
		try {
			Tuple tuple = new Tuple();
			Chain chain = new Chain();
			Chain supportChain = new Chain();
			ActionContext ac = new ActionsExecutor(context, null);

			while (true) {
				chainsToSend.remove(chain);
				chain.getInputTuple(tuple);

				ChainLocation loc = context
						.getInputLayer(chain.getInputLayer()).getLocations(
								tuple, ac);

				NetworkLayer ibis = context.getNetworkLayer();
				IbisIdentifier[] nodes = ibis.getPeersLocation(loc);

				if (nodes.length == 0) { // Put it directly in the queue
					chainsToProcess.add(chain);
				} else { // Send the chains

					int i = nodes.length - 1;
					while (i != 0) {
						chain.branch(supportChain, context
								.getUniqueCounter(Consts.CHAINCOUNTER_NAME));

						if (nodes[i].compareTo(ibis.ibis.identifier()) == 0) {
							chainsToProcess.add(supportChain);
						} else {
							WriteMessage msg = ibis.getMessageToSend(nodes[i],
									NetworkLayer.nameMgmtReceiverPort);
							msg.writeByte((byte) 0);
							supportChain.writeTo(new WriteMessageWrapper(msg));
							ibis.finishMessage(msg,
									supportChain.getSubmissionId());
						}
						i--;
					}

					if (nodes[0].compareTo(ibis.ibis.identifier()) == 0) {
						chainsToProcess.add(chain);
					} else {
						WriteMessage msg = ibis.getMessageToSend(nodes[0],
								NetworkLayer.nameMgmtReceiverPort);
						msg.writeByte((byte) 0);
						chain.writeTo(new WriteMessageWrapper(msg));
						ibis.finishMessage(msg, chain.getSubmissionId());
					}
				}
			}
		} catch (Exception e) {
			log.error("Error in the main execution of the communicator thread",
					e);
		}
	}
}
