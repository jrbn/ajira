package arch.net;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.WriteMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.Context;
import arch.chains.Chain;
import arch.chains.ChainLocation;
import arch.data.types.Tuple;
import arch.statistics.StatisticsCollector;
import arch.storage.Container;

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

			while (true) {
				chainsToSend.remove(chain);
				chain.getInputTuple(tuple);

				ChainLocation loc = context.getInputLayer(chain.getInputLayerId())
						.getLocations(tuple, chain, context);

				NetworkLayer ibis = context.getNetworkLayer();
				IbisIdentifier[] nodes = ibis.getPeersLocation(loc);

				chain.setReplicatedFactor(Math.max(nodes.length, 1));
				if (nodes.length == 0) { // Put it directly in the queue
					chainsToProcess.add(chain);
				} else { // Send the chains
					for (int i = 0; i < nodes.length; ++i) {
						IbisIdentifier node = nodes[i];
						if (i > 0) {
							chain.setChainChildren(0);
							chain.setReplicatedFactor(0);
						}
						if (node.compareTo(ibis.ibis.identifier()) == 0) {
							chainsToProcess.add(chain);
						} else {
							WriteMessage msg = ibis.getMessageToSend(node,
									NetworkLayer.nameMgmtReceiverPort);
							msg.writeByte((byte) 0);
							chain.writeTo(new WriteMessageWrapper(msg));
							ibis.finishMessage(msg, chain.getSubmissionId());
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("Error in the main execution of the communicator thread",
					e);
		}
	}
}
