package nl.vu.cs.ajira.mgmt;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsCollector {

	protected static final Logger log = LoggerFactory
			.getLogger(StatisticsCollector.class);
	private final Map<Integer, Map<Integer, Map<String, Long>>> counters = new HashMap<Integer, Map<Integer, Map<String, Long>>>();

	private boolean statsEnabled;
	private NetworkLayer net;
	private int myId;

	public StatisticsCollector(Configuration conf, NetworkLayer net) {
		this.net = net;
		statsEnabled = conf.getBoolean(Consts.STATS_ENABLED, true);
		myId = net.getMyPartition();
	}

	public synchronized Map<String, Long> removeCountersSubmission(
			int idSubmission) {
		Map<Integer, Map<String, Long>> mines = counters.get(myId);
		if (mines != null)
			return mines.remove(idSubmission);
		else
			return null;
	}

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
				c = new HashMap<String, Long>();
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
