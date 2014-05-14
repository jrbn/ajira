package nl.vu.cs.ajira.net;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.buckets.WritableTuple;
import nl.vu.cs.ajira.chains.Chain;
import nl.vu.cs.ajira.chains.Location;
import nl.vu.cs.ajira.mgmt.StatisticsCollector;
import nl.vu.cs.ajira.storage.Container;
import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.containers.CheckedConcurrentWritableContainer;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class provides methods that facilitate the communication between nodes.
 * It creates ports, sends messages, signals.
 * 
 */
public class NetworkLayer {

	static final Logger log = LoggerFactory.getLogger(NetworkLayer.class);

	public static final String queryReceiverPort = "query-receiver-port";
	public static final String nameMgmtReceiverPort = "mgmt-receiver-port";
	public static final String nameBcstReceiverPort = "bcst-receiver-port";
	public static final String nameReceiverPort = "receiver-port";

	static final PortType requestPortType = new PortType(
			PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA,
			PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_AUTO_UPCALLS);

	public static final PortType mgmtRequestPortType = new PortType(
			PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT,
			PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_AUTO_UPCALLS);

	static final PortType broadcastPortType = new PortType(
			PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT_SUN,
			PortType.CONNECTION_MANY_TO_MANY, PortType.RECEIVE_AUTO_UPCALLS);

	public static final PortType queryAnswerPortType = new PortType(
			PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA,
			PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_EXPLICIT);

	public static final IbisCapabilities ibisClusterCapabilities = new IbisCapabilities(
			IbisCapabilities.ELECTIONS_STRICT,
			IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED,
			IbisCapabilities.SIGNALS, IbisCapabilities.CLOSED_WORLD);

	public static final IbisCapabilities ibisServerCapabilities = new IbisCapabilities(
			IbisCapabilities.ELECTIONS_STRICT,
			IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED,
			IbisCapabilities.MALLEABLE);

	Ibis clusterIbis = null;
	Ibis clientIbis = null;
	private int partitionId = 0;
	private int poolSize;
	private IbisIdentifier[] assignedPartitions = null;
	private final Map<String, Integer> assignedIds = new HashMap<String, Integer>();
	private final Set<ReceivePort> receivePorts = new HashSet<ReceivePort>();
	private final Map<String, SendPort> senderPorts = new ConcurrentHashMap<String, SendPort>();
	private final Map<String, Long> timers = new ConcurrentHashMap<String, Long>();

	private boolean serverMode = false;
	private boolean clusterMode = false;
	private IbisIdentifier server = null;
	private StatisticsCollector stats = null;

	ChainSender sender;
	TupleRequester tupleRequester;
	TupleSender tupleSender;
	Receiver receiver;
	Container<Chain> chainsToSend = new CheckedConcurrentWritableContainer<Chain>(
			Consts.SIZE_BUFFERS_CHAIN_SEND);

	Factory<WritableContainer<WritableTuple>> bufferFactory = null;

	protected boolean monitorCounters = false;

	protected int statsCount;

	SendPort broadcastPort;

	private IbisMonitor ibisMonitor;
	private final static NetworkLayer instance = new NetworkLayer();

	ChainTerminator terminator;

	private ReceivePort clientReceivePort;

	/**
	 * Creates a new NetworkLayer object.
	 */
	private NetworkLayer() {
	}

	/**
	 * Sets the bufferFactory of the class.
	 * 
	 * @param bufferFactory
	 *            The factory used for generating buffers (buffer allocation and
	 *            memory management).
	 */
	public void setBufferFactory(
			Factory<WritableContainer<WritableTuple>> bufferFactory) {
		this.bufferFactory = bufferFactory;
	}

	/**
	 * 
	 * @return The NetworkLayer instance of the class.
	 */
	public static NetworkLayer getInstance() {
		return instance;
	}

	/*********** PUBLIC INTERFACE ****************/

	/**
	 * 
	 * @return The server.
	 */
	public IbisIdentifier getServer() {
		return server;
	}

	/**
	 * Adds a new chain at the list of chains that need to be send.
	 * 
	 * @param chain
	 *            The chain that is added at the list.
	 */
	public void sendChain(Chain chain) {
		chainsToSend.add(chain);
	}

	/**
	 * Adds the chains from the parameter to the list of chains that need to be
	 * send.
	 * 
	 * @param chainsToProcess
	 *            A container of chains that need to be processed.
	 */
	public void sendChains(WritableContainer<Chain> chainsToProcess) {
		chainsToSend.addAll(chainsToProcess);
	}

	/**
	 * 
	 * @param chain
	 *            The chain that is added at the ChainTerminator.
	 */
	public void signalChainTerminated(Chain chain,
			Map<Long, List<Integer>> additionalCounters) {
		terminator.addChain(chain, additionalCounters);
	}

	public void signalsBucketToFetch(int idSubmission, int submissionNode,
			int idBucket, int remoteNodeId, long bufferKey, boolean streaming) {
		// called from upcall thread, so no new thread needed.
		tupleRequester.handleNewRequest(idSubmission, submissionNode, idBucket,
				remoteNodeId, bufferKey, 0, 0, streaming);
	}

	public void signalsBucketToFetch(int idSubmission, int submissionNode,
			int idBucket, int remoteNodeId, long bufferKey, int sequence,
			int nrequest) {
		tupleRequester.handleNewRequest(idSubmission, submissionNode, idBucket,
				remoteNodeId, bufferKey, sequence, nrequest);
	}

	public void addRequestToSendTuples(long bucketKey, int remoteNodeId,
			int submissionId, int submissionNode, int bucketId, long ticket,
			int sequence, int nrequest) {
		tupleSender.handleNewRequest(bucketKey, remoteNodeId, submissionId,
				submissionNode, bucketId, ticket, sequence, nrequest);
	}

	public void removeActiveRequest(long ticket) {
		tupleRequester.removeActiveRequest(ticket);
	}

	public boolean isServer() {
		return serverMode;
	}

	/**
	 * Creates a new Ibis instance. Waits for all the ibises from the pool to
	 * join. Keeps track of the identifier of every Ibis. The server is elected
	 * to be on the first partition. A monitor is created for each ibis.
	 * 
	 * @throws Exception
	 */
	public void startIbis(boolean clusterMode) throws Exception {
		this.clusterMode = clusterMode;
		if (clusterIbis == null) {
			clusterIbis = IbisFactory.createIbis(ibisClusterCapabilities, null,
					requestPortType, mgmtRequestPortType, broadcastPortType);

			clusterIbis.registry().waitUntilPoolClosed();
			poolSize = clusterIbis.registry().getPoolSize();

			assignedPartitions = clusterIbis.registry().joinedIbises();

			// Sort on IbisIdentifier, which sorts on pool and then
			// location.
			// This at least gives a fixed order which may help in placing
			// caches ...
			Arrays.sort(assignedPartitions);
			for (int i = 0; i < assignedPartitions.length; i++) {
				if (assignedPartitions[i].equals(clusterIbis.identifier())) {
					partitionId = i;
					if (log.isInfoEnabled()) {
						log.info("Assigned partition " + i + " to "
								+ clusterIbis.identifier());
					}
				}
				assignedIds.put(assignedPartitions[i].name(), i);
			}

			// Put the server on partition 0.
			if (assignedPartitions[0].equals(clusterIbis.identifier())) {
				server = clusterIbis.registry().elect("server");
				serverMode = true;
			} else {
				server = clusterIbis.registry().getElectionResult("server");
			}

			if (serverMode && clusterMode) {
				// Start an Ibis to server query requests.
				Properties p = new Properties(System.getProperties());
				p.setProperty("ibis.pool.name",
						System.getProperty("ibis.pool.name") + "-server");
				clientIbis = IbisFactory.createIbis(ibisServerCapabilities, p,
						true, null, mgmtRequestPortType, queryAnswerPortType);
				clientIbis.registry().elect("server");
			}

			if (log.isDebugEnabled()) {
				log.debug("I AM " + clusterIbis.identifier() + ", partition "
						+ partitionId);
			}
		}
		try {
			ibisMonitor = IbisMonitor.createMonitor(clusterIbis);
		} catch (Throwable e) {
			if (log.isInfoEnabled()) {
				log.info("Could not create IbisMonitor instance", e);
			}
		}
	}

	/**
	 * Starts the threads corresponding to the ChainTerminator and ChainSender.
	 * Enables the connections for the ports and creates ports between each pair
	 * of ibises.
	 * 
	 * @param context
	 *            Current context.
	 */
	public void startupConnections(Context context) throws Exception {
		stats = context.getStatisticsCollector();

		/**** START SUBMISSION MANAGEMENT THREAD ****/
		if (log.isDebugEnabled()) {
			log.debug("Starting Termination chains thread...");
		}
		terminator = new ChainTerminator(context);
		Thread thread = new Thread(terminator);
		thread.setName("Chain Terminator");
		thread.start();

		if (context.isLocalMode()) {
			return;
		}

		sender = new ChainSender(context, chainsToSend);
		thread = new Thread(sender);
		thread.setName("Chain Sender");
		thread.start();

		tupleRequester = new TupleRequester(context);
		tupleSender = new TupleSender(context);

		receiver = new Receiver(context, bufferFactory);

		ReceivePort port = clusterIbis.createReceivePort(requestPortType,
				nameReceiverPort, receiver);
		port.enableConnections();
		port.enableMessageUpcalls();
		receivePorts.add(port);

		port = clusterIbis.createReceivePort(mgmtRequestPortType,
				nameMgmtReceiverPort, receiver);
		port.enableConnections();
		port.enableMessageUpcalls();
		receivePorts.add(port);
		if (log.isDebugEnabled()) {
			log.debug("Mgmt receiver port is created");
		}

		port = clusterIbis.createReceivePort(broadcastPortType,
				nameBcstReceiverPort, receiver);
		port.enableConnections();
		port.enableMessageUpcalls();
		receivePorts.add(port);
		if (log.isDebugEnabled()) {
			log.debug("Broadcast receiver port is created");
		}

		// Start a broadcast port
		broadcastPort = clusterIbis.createSendPort(broadcastPortType);

		// Connect every node with all the others and put the ports in
		// sendPorts
		for (IbisIdentifier peer : assignedPartitions) {
			String nameSenderPort = nameReceiverPort + peer.name();
			startSenderPort(requestPortType, nameSenderPort, peer,
					nameReceiverPort);

			nameSenderPort = nameMgmtReceiverPort + peer.name();
			startSenderPort(mgmtRequestPortType, nameSenderPort, peer,
					nameMgmtReceiverPort);

			if (!peer.equals(clusterIbis.identifier())) {
				broadcastPort.connect(peer, nameBcstReceiverPort);
			}
		}

		if (clientIbis != null) {
			clientReceivePort = clientIbis.createReceivePort(
					mgmtRequestPortType, queryReceiverPort, receiver);
			clientReceivePort.enableConnections();
			clientReceivePort.enableMessageUpcalls();
		}
	}

	/**
	 * 
	 * @return The id of the partition. Is the position in the
	 *         assignedPartitions array of the clusterIbis.
	 */
	public int getMyPartition() {
		return partitionId;
	}

	/**
	 * 
	 * @return The number of partitions (ibisses).
	 */
	public int getNumberNodes() {
		if (assignedPartitions == null)
			return 1;
		else
			return assignedPartitions.length;
	}

	/**
	 * Closes the receiving ports and stops Ibis.
	 * 
	 * @throws IOException
	 */
	public void stopIbis() throws IOException {
		for (ReceivePort rp : receivePorts) {
			try {
				rp.close(-1);
			} catch (Throwable e) {
				// ignore
			}
		}

		clusterIbis.end();

		if (clientIbis != null) {
			try {
				clientReceivePort.close(-1);
			} catch (Throwable e) {
				// ignore
			}
			clientIbis.end();
		}
	}

	/**
	 * 
	 * @param receiver
	 *            The identifier of the Ibis that will receive the message.
	 * @return A new WriteMessage object constructed for the parameters of the
	 *         method.
	 * @throws IOException
	 */
	public WriteMessage getMessageToSend(IbisIdentifier receiver)
			throws IOException {
		return getMessageToSend(receiver, nameReceiverPort);
	}

	/**
	 * Creates a new message for the port that has the name of the receiverPort
	 * concatenated with the name of the receiver. If such a port does not
	 * exists it is created.
	 * 
	 * @param receiver
	 *            The identifier of the Ibis that will receive the message.
	 * @param receiverPort
	 *            The name of the port.
	 * @return A new WriteMessage object constructed for the parameters of the
	 *         method.
	 * @throws IOException
	 */
	public WriteMessage getMessageToSend(IbisIdentifier receiver,
			String receiverPort) throws IOException {

		SendPort port = null;

		String nameSenderPort = receiverPort + receiver.name();
		if (!senderPorts.containsKey(nameSenderPort)) {
			synchronized (NetworkLayer.class) {
				if (!senderPorts.containsKey(nameSenderPort)) {
					PortType type = null;
					if (receiverPort.equals(queryReceiverPort)) {
						type = queryAnswerPortType;
					} else if (receiverPort.equals(nameMgmtReceiverPort)) {
						type = mgmtRequestPortType;
					} else if (receiverPort.equals(nameBcstReceiverPort)) {
						type = broadcastPortType;
					} else {
						type = requestPortType;
					}
					startSenderPort(type, nameSenderPort, receiver,
							receiverPort);
				}
			}
		}
		port = senderPorts.get(nameSenderPort);
		WriteMessage w = port.newMessage();
		timers.put(nameSenderPort, System.currentTimeMillis());
		return w;
	}

	/**
	 * 
	 * @param index
	 *            The position of the wanted IbisIdentifier.
	 * @return The IbisIdentifier found at the specified position.
	 */
	public IbisIdentifier getPeerLocation(int index) {
		return assignedPartitions[index];
	}

	/**
	 * 
	 * @param id
	 *            The identifier of the ibis.
	 * @return The id of the IbisIdentifier.
	 */
	public int getPeerId(IbisIdentifier id) {
		return assignedIds.get(id.name());
	}

	/**
	 * 
	 * @param loc
	 * 
	 * @return All or some of the IbisIdentifiers depending on the parameter.
	 */
	public IbisIdentifier[] getPeersLocation(Location loc) {
		if (loc == Location.ALL_NODES) {
			return assignedPartitions;
		} else if (loc == Location.THIS_NODE) {
			return Arrays.copyOfRange(assignedPartitions, partitionId,
					partitionId + 1);
		} else {
			return Arrays.copyOfRange(assignedPartitions, loc.getStart(),
					loc.getEnd() + 1);
		}
	}

	/**
	 * 
	 * Creates a new SendPort and connects to the receiver.
	 * 
	 * @param senderPortType
	 *            The type of the sender port.
	 * @param senderPort
	 *            The name of the sender port.
	 * @param receiver
	 *            The Ibis instance that receives messages.
	 * @param receiverPort
	 *            The name of the receiver.
	 * @return The port that is created.
	 */
	private SendPort startSenderPort(PortType senderPortType,
			String senderPort, IbisIdentifier receiver, String receiverPort) {

		SendPort port = null;
		Ibis ibis = receiverPort.equals(queryReceiverPort) ? clientIbis
				: clusterIbis;
		try {
			port = ibis.createSendPort(senderPortType, senderPort);
			port.connect(receiver, receiverPort);
			if (port.connectedTo() != null) {
				senderPorts.put(senderPort, port);
				return port;
			} else {
				return null; // Connected to any resource
			}

		} catch (Exception e) {
			log.error("Failed in creating the sender port " + senderPort
					+ "to node " + receiver, e);
		}

		return port;
	}

	/**
	 * Sends a message to all the peers, except itself, telling them to
	 * terminate.
	 */
	public void signalTermination() {
		for (IbisIdentifier peer : assignedPartitions) {
			if (!peer.equals(clusterIbis.identifier())) {
				WriteMessage msg = null;
				try {
					msg = getMessageToSend(peer, nameMgmtReceiverPort);
					msg.writeByte((byte) 3);
					msg.finish();
					if (log.isDebugEnabled()) {
						log.debug("Sent message with id 3 to " + peer);
					}
				} catch (IOException e) {
					log.error("Could not send termination message to " + peer,
							e);
					if (msg != null) {
						msg.finish(e);
					}
				}
			}
		}
	}

	/**
	 * 
	 * @return The number of bytes sent.
	 */
	public long getSentBytes() {
		try {
			return Long.parseLong(clusterIbis
					.getManagementProperty("bytesSent"));
		} catch (Exception e) {
			log.error("getSentBytes error", e);
			return 0;
		}
	}

	/**
	 * 
	 * @return The number of messages sent.
	 */
	public long getSentMessages() {
		try {
			return Long.parseLong(clusterIbis
					.getManagementProperty("outgoingMessageCount"));
		} catch (Exception e) {
			log.error("getSentMessages error", e);
			return 0;
		}
	}

	/**
	 * 
	 * @return The number of bytes received.
	 */
	public long getReceivedBytes() {
		try {
			return Long.parseLong(clusterIbis
					.getManagementProperty("bytesReceived"));
		} catch (Exception e) {
			log.error("getSentBytes error", e);
			return 0;
		}
	}

	/**
	 * 
	 * @return The number of messages received.
	 */
	public long getReceivedMessages() {
		try {
			return Long.parseLong(clusterIbis
					.getManagementProperty("incomingMessageCount"));
		} catch (Exception e) {
			log.error("getSentMessages error", e);
			return 0;
		}
	}

	/**
	 * Stops monitoring Ibis.
	 */
	public void stopMonitorCounters() {
		if (ibisMonitor != null) {
			ibisMonitor.setMonitoring(false);
		}
	}

	/**
	 * Starts monitoring Ibis.
	 */
	public synchronized void startMonitorCounters() {
		if (ibisMonitor != null) {
			ibisMonitor.setMonitoring(true);
		}
	}

	/**
	 * Finishes sending the message msg and adds at the counters informations
	 * about the sending time and the bytes that were sent.
	 * 
	 * @param msg
	 *            The message that was sent.
	 * @param submissionId
	 *            The subbmission id.
	 * @throws IOException
	 */
	public void finishMessage(WriteMessage msg, int submissionId)
			throws IOException {
		SendPort p = msg.localPort();
		long bytes = msg.finish();
		long startTime = timers.get(p.name());
		stats.addCounter(0, submissionId,
				"NetworkLayer: time sending msgs (ms)",
				System.currentTimeMillis() - startTime);
		stats.addCounter(0, submissionId, "NetworkLayer: bytes sent", bytes);
		// stats.addCounter(0, submissionId, "Messages sent", 1);
	}

	public static class CountInfo {
		int count;
		List<Object[]> receivedObjects = new ArrayList<Object[]>();
		boolean success = true;
	}

	int activeBroadcastCount = 0;
	Map<Integer, CountInfo> activeBroadcasts = new HashMap<Integer, CountInfo>();
	int activeRetrievalCount = 0;
	Map<Integer, CountInfo> activeRetrievals = new HashMap<Integer, CountInfo>();
	int activeCodeExecutionsCount = 0;
	Map<Integer, CountInfo> activeCodeExecutions = new HashMap<Integer, CountInfo>();

	private int readyCount;

	/**
	 * 
	 * @return A WriteMessge object for the broadcast port.
	 * @throws IOException
	 */
	public WriteMessage getMessageToBroadcast() throws IOException {
		return broadcastPort.newMessage();
	}

	/**
	 * 
	 * @param submissionId
	 *            The submission id.
	 * @param keys
	 *            The array of key Objects that is broadcast.
	 * @return The list of remaining objects received.
	 */
	public List<Object[]> retrieveObjects(int submissionId, Object[] keys) {
		try {
			int c;
			CountInfo remaining = new CountInfo();
			remaining.count = assignedPartitions.length - 1;

			synchronized (activeRetrievals) {
				c = activeRetrievalCount++;
				activeRetrievals.put(c, remaining);
			}

			WriteMessage msg = broadcastPort.newMessage();
			msg.writeByte((byte) 12);
			msg.writeInt(c);
			msg.writeInt(submissionId);
			msg.writeObject(keys);
			msg.finish();

			synchronized (remaining) {
				while (remaining.count > 0) {
					remaining.wait();
				}
			}

			synchronized (activeRetrievals) {
				activeRetrievals.remove(c);
			}

			// Return the objects
			return remaining.receivedObjects;

		} catch (Exception e) {
			log.error("Failed retrieving objects", e);
		}

		return null;
	}

	/**
	 * Broadcasts the arrays of objects from the parameters.
	 * 
	 * @param submissionId
	 *            The submission id.
	 * @param keys
	 *            The array of key Objects that is broadcast.
	 * @param values
	 *            The array of value Objects that is broadcast.
	 * @throws IOException
	 */
	public void broadcastObjects(int submissionId, Object[] keys,
			Object[] values) throws IOException {

		int c;
		CountInfo remaining = new CountInfo();
		remaining.count = assignedPartitions.length - 1;
		synchronized (activeBroadcasts) {
			c = activeBroadcastCount++;
			activeBroadcasts.put(c, remaining);
		}

		WriteMessage msg = broadcastPort.newMessage();
		msg.writeByte((byte) 10);
		msg.writeInt(c);
		msg.writeInt(submissionId);
		msg.writeObject(keys);
		msg.writeObject(values);
		msg.finish();

		synchronized (remaining) {
			while (remaining.count > 0) {
				try {
					remaining.wait();
				} catch (InterruptedException e) {
					// ignore
				}
			}
		}
		synchronized (activeBroadcasts) {
			activeBroadcasts.remove(c);
		}
	}

	/**
	 * Broadcasts the objects from the parameters.
	 * 
	 * @param submissionId
	 *            The submission id.
	 * @param key
	 *            The key Object that is broadcast.
	 * @param value
	 *            The value Object that is broadcast.
	 */
	public void broadcastObject(int submissionId, Object key, Object value) {
		try {
			int c;
			CountInfo remaining = new CountInfo();
			remaining.count = assignedPartitions.length - 1;
			synchronized (activeBroadcasts) {
				c = activeBroadcastCount++;
				activeBroadcasts.put(c, remaining);
			}

			WriteMessage msg = broadcastPort.newMessage();
			msg.writeByte((byte) 10);
			msg.writeInt(c);
			msg.writeInt(submissionId);
			msg.writeObject(new Object[] { key });
			msg.writeObject(new Object[] { value });
			msg.finish();

			synchronized (remaining) {
				while (remaining.count > 0) {
					remaining.wait();
				}
			}
			synchronized (activeBroadcasts) {
				activeBroadcasts.remove(c);
			}
		} catch (Exception e) {
			log.error("Failed broadcasting object", e);
		}
	}

	/**
	 * It broadcast to every Ibis to start monitoring.
	 * 
	 * @throws IOException
	 */
	public void broadcastStartMonitoring() throws IOException {
		if (ibisMonitor != null) {
			WriteMessage msg = broadcastPort.newMessage();
			msg.writeByte((byte) 16);
			msg.finish();
		}
	}

	/**
	 * It broadcast to every Ibis to stop monitoring.
	 * 
	 * @throws IOException
	 */
	public void broadcastStopMonitoring() throws IOException {
		if (ibisMonitor != null) {
			WriteMessage msg = broadcastPort.newMessage();
			msg.writeByte((byte) 17);
			msg.finish();
		}
	}

	/**
	 * It sends the objects key and value to the node with the id nodeId.
	 * 
	 * @param submissionId
	 *            The submission id.
	 * @param nodeId
	 *            The node id.
	 * @param key
	 *            The key Object that is send.
	 * @param value
	 *            The value Object that is send.
	 * @throws IOException
	 */
	public void sendObject(int submissionId, int nodeId, Object key,
			Object value) throws IOException {
		IbisIdentifier id = getPeerLocation(nodeId);
		WriteMessage msg = getMessageToSend(id, nameMgmtReceiverPort);
		timers.put(msg.localPort().name(), System.currentTimeMillis());
		int c;
		CountInfo remaining = new CountInfo();
		remaining.count = 1;
		synchronized (activeBroadcasts) {
			c = activeBroadcastCount++;
			activeBroadcasts.put(c, remaining);
		}

		msg.writeByte((byte) 10);
		msg.writeInt(c);
		msg.writeInt(submissionId);
		msg.writeObject(new Object[] { key });
		msg.writeObject(new Object[] { value });
		finishMessage(msg, submissionId);

		synchronized (remaining) {
			while (remaining.count > 0) {
				try {
					remaining.wait();
				} catch (InterruptedException e) {
					// ignore
				}
			}
		}
		synchronized (activeBroadcasts) {
			activeBroadcasts.remove(c);
		}
	}

	/**
	 * Adds the chain to the list of terminated chains.
	 * 
	 * @param chain
	 *            The chain that is added at the list of terminated chains.
	 * @param exception
	 *            The exception thrown when the chain failed.
	 */
	public void signalChainFailed(Chain chain, Throwable exception) {
		terminator.addFailedChain(chain, exception);
	}

	public void waitUntilAllReady() {
		int n = poolSize - 1;
		int currentSignals = 0;
		while (currentSignals < n) {
			String[] signals = clusterIbis.registry().receivedSignals();
			if (signals != null) {
				for (String signal : signals) {
					if (signal.equalsIgnoreCase("ready")) {
						currentSignals++;
					}
				}
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// ignore
			}
		}
	}

	public void signalReady() throws IOException {
		clusterIbis.registry().signal("ready", server);
	}
}
