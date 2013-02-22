package nl.vu.cs.ajira;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.net.WriteMessageWrapper;
import nl.vu.cs.ajira.submissions.Job;

/**
 * A client for an Ajira cluster.
 */
public class AjiraClient {
	
	private final Ibis ibis;
	private final IbisIdentifier server;
	private final ReceivePort rp;
	private final SendPort sp;
	
	/**
	 * Constructs an Ajira client from an Ajira cluster described by the specified
	 * properties file.
	 * @param filename the cluster properties.
	 * @throws IOException is thrown when the client creation fails for some reason.
	 */
	public AjiraClient(String filename) throws IOException {
		
		Properties properties = new Properties();
		
		properties.load(new BufferedInputStream(new FileInputStream(filename)));
		
		// Create an ibis and determine which node to connect to.
		try {
			ibis = IbisFactory.createIbis(NetworkLayer.ibisCapabilities, properties, true, null,
					NetworkLayer.mgmtRequestPortType, NetworkLayer.queryPortType);
		} catch (IbisCreationFailedException e) {
			throw new IOException("Could not create Ibis", e);
		}
		server = ibis.registry().getElectionResult("server");

		// Create a receive port to receive answers from the cluster.
		rp = ibis.createReceivePort(NetworkLayer.queryPortType,
				NetworkLayer.queryReceiverPort);
		rp.enableConnections();
		
		// And finally create a sendport and connect.
		sp = ibis.createSendPort(NetworkLayer.mgmtRequestPortType);
		sp.connect(server, NetworkLayer.nameMgmtReceiverPort);
	}
	
	public synchronized void submitJob(Job job) throws IOException {
		WriteMessage msg = sp.newMessage();
        msg.writeByte((byte) 7);
        job.writeTo(new WriteMessageWrapper(msg));
        msg.finish();

        // TODO: can we run only one job at a time, or should jobs get an
        // identifier?
        // TODO: wait here for answer or have a separate method to get the answer?
	}
}
