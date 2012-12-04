package arch;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.chains.Chain;
import arch.net.NetworkLayer;
import arch.storage.Container;
import arch.utils.Configuration;
import arch.utils.Consts;

public class StatisticsCollector implements Runnable {

    static final Logger log = LoggerFactory
	    .getLogger(StatisticsCollector.class);

    Configuration conf;
    boolean statsEnabled;
    NetworkLayer net;
    int myId;
    Container<Chain> chainsToProcess;

    private final Map<Integer, Map<Integer, Map<String, Long>>> counters = new HashMap<Integer, Map<Integer, Map<String, Long>>>();

    public StatisticsCollector(Configuration conf, NetworkLayer net,
	    Container<Chain> chainsToProcess) {
	this.conf = conf;
	this.net = net;
	this.chainsToProcess = chainsToProcess;
	statsEnabled = conf.getBoolean(Consts.STATS_ENABLED, true);
	myId = net.getMyPartition();
    }

    @Override
    public void run() {

	int statisticsInterval = conf.getInt(Consts.STATISTICAL_INTERVAL,
		Consts.DEFAULT_STATISTICAL_INTERVAL);

	while (true) {
	    try {
		sendStatisticsAway();
		Thread.sleep(statisticsInterval);
	    } catch (Exception e) {
		log.error("Exception", e);
	    }
	}
    }

    public synchronized Map<String, Long> removeCountersSubmission(
	    int idSubmission) {
	Map<Integer, Map<String, Long>> mines = counters.get(myId);
	return mines.remove(idSubmission);
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
			if (log.isDebugEnabled()) {
			    printStatistics(entry2.getKey(), entry.getKey().intValue());
			}
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
		    // if (log.isDebugEnabled()) {
		    // log.debug("Sent message id 6 to " + receiver);
		    // }
		    msg = null;
		}
	    }
	}
    }
    
    public void printStatistics(int submissionId) {
    	printStatistics(submissionId, myId);
    }

    public synchronized void printStatistics(int submissionId, int nodeId) {
    	Map<Integer, Map<String, Long>> localMap = counters.get(nodeId);
    	if (localMap != null) {
    		String stats = "";
    		// for (Map.Entry<Integer, Map<String, Long>> entry : localMap
    		// .entrySet()) {
    		stats += "\nSubmission ID: " + submissionId + "\n";
    		
    		Map<String, Long> submissionCounters = localMap.get(submissionId);
    		Map<String, Long> sortedSubmissionCounters = 
    				new TreeMap<String, Long>(submissionCounters);
    		
    		if (sortedSubmissionCounters != null) {
    			for (Map.Entry<String, Long> entry2 : 
    				sortedSubmissionCounters.entrySet()) {
	    				stats += " " + entry2.getKey() + " = " + entry2.getValue()
	    						+ "\n";
    			}
    		}
    		// }
    		log.info(stats);
    	}
    }
}
