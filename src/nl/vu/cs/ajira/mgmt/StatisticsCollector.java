package nl.vu.cs.ajira.mgmt;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class it is used to keep track of different statistics
 * for the nodes. For example it can keep track of the bytes 
 * sent by messages or the time to sent a messages.
 *
 */
public class StatisticsCollector {

	protected static final Logger log = LoggerFactory
			.getLogger(StatisticsCollector.class);
	private final Map<Integer, Map<Integer, Map<String, Long>>> counters = new HashMap<Integer, Map<Integer, Map<String, Long>>>();

	private boolean statsEnabled;
	private NetworkLayer net;
	private int myId;

	/**
	 * Custom constructor. It keeps for each node
	 * the configuration of the cluster, the network
	 * and the id of the node. 
	 * 
	 * @param conf
	 * 		The configuration of the cluster.
	 * @param net
	 * 		The NetworkLayer of the cluster.
	 */
	public StatisticsCollector(Configuration conf, NetworkLayer net) {
		this.net = net;
		statsEnabled = conf.getBoolean(Consts.STATS_ENABLED, true);
		myId = net.getMyPartition();
	}

	/**
	 * 
	 * @param submission
	 * 		The submission id.
	 * @return
	 * 		Returns a node's map for a submission id with 
	 * 		all its counters and their values.
	 */
	public synchronized Map<String, Long> getCounters(int submission) {
		Map<Integer, Map<String, Long>> mines = counters.get(myId);
		if (mines != null) {
			return mines.get(submission);
		}
		return null;
	}

	/**
	 * Removes the map corresponding to the node 
	 * and the submission id.
	 * 
	 * @param idSubmission
	 * 		The submission id.
	 * @return
	 * 		The map for the node and the submission id 
	 * 		that it is removed. 
	 */
	public synchronized Map<String, Long> removeCountersSubmission(
			int idSubmission) {
		Map<Integer, Map<String, Long>> mines = counters.get(myId);
		if (mines != null)
			return mines.remove(idSubmission);
		else
			return null;
	}

	/**
	 * If the statistics collector is enabled
	 * it adds or it updates the counter for a
	 * specific node id and submission id.
	 * 
	 * @param nodeId
	 * 		The id of the node.
	 * @param submissionId
	 * 		The submission id.
	 * @param nameCounter
	 * 		The name of the counter that is added or updated.
	 * @param value
	 * 		The value of the counter.
	 */
	public synchronized void addCounter(int nodeId, int submissionId,
			String nameCounter, long value) {

		if (statsEnabled) {
			Map<Integer, Map<String, Long>> submissionsCounters = counters
					.get(nodeId);
			if (submissionsCounters == null) {
				submissionsCounters = new HashMap<Integer, Map<String, Long>>();
				counters.put(nodeId, submissionsCounters);
			}

			Map<String, Long> c = submissionsCounters.get(submissionId);
			if (c == null) {
				c = new TreeMap<String, Long>();
				submissionsCounters.put(submissionId, c);
			}

			long oValue = 0;
			if (c.containsKey(nameCounter)) {
				oValue = c.get(nameCounter);
			}
			oValue += value;
			c.put(nameCounter, oValue);
		}
	}

	/**
	 * For all the nodes, except the current node,
	 * and all the submission ids it sends a message.
	 * The message is composed of 1, the submission id, 
	 * the number of counters, the number of bytes of 
	 * the counter's name, the counter's name, it's 
	 * values and 0. It resets the map for the counters.
	 * @throws IOException
	 */
	public synchronized void sendStatisticsAway() throws IOException {

		for (Map.Entry<Integer, Map<Integer, Map<String, Long>>> entry : counters
				.entrySet()) {
			// If node is not localhost
			if (entry.getKey().intValue() != myId) {

				WriteMessage msg = null;
				IbisIdentifier receiver = null;

				// For every submission
				for (Map.Entry<Integer, Map<String, Long>> entry2 : entry
						.getValue().entrySet()) {

					Map<String, Long> submissionCounters = entry2.getValue();
					// If there are counters
					if (submissionCounters.size() > 0) {
						if (msg == null) {
							receiver = net.getPeerLocation(entry.getKey());
							msg = net.getMessageToSend(receiver,
									NetworkLayer.nameMgmtReceiverPort);
							msg.writeByte((byte) 6);
						}

						// Write the submission Id
						msg.writeByte((byte) 1);

						msg.writeInt(entry2.getKey());
						msg.writeInt(submissionCounters.size());

						for (Map.Entry<String, Long> entry3 : submissionCounters
								.entrySet()) {
							byte[] key = entry3.getKey().getBytes();
							msg.writeInt(key.length);
							msg.writeArray(key);
							msg.writeLong(entry3.getValue());
						}
						submissionCounters.clear();

					}
				}

				if (msg != null) {
					msg.writeByte((byte) 0);
					msg.finish();
					msg = null;
				}
			}
		}
	}
}
