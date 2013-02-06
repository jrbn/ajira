package nl.vu.cs.ajira.net;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.WriteMessage;

import java.util.HashSet;
import java.util.Set;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.storage.Factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to handle/send a request for 
 * tuples transfer (remote-bucket's data fetch).
 */
class TupleRequester {
	static final Logger log = LoggerFactory.getLogger(TupleRequester.class);

	private NetworkLayer net;
	private long ticket = 0;
	private Set<Long> activeRequests = new HashSet<Long>();
	private Factory<TupleInfo> tuFactory = new Factory<TupleInfo>(
			TupleInfo.class);

	/**
	 * Custom constructor.
	 * 
	 * @param context
	 * 			  The current context
	 */
	public TupleRequester(Context context) {
		this.net = context.getNetworkLayer();
	}

	/**
	 * This method is used to handle a new request 
	 * for a chunk transfer (from a remote-bucket).
	 * 
	 * INFO: This method should be called from a 
	 * thread that may block.
	 * 
	 * @param idSubmission
	 * 			  Submission id
	 * @param idBucket
	 * 			  Bucket id
	 * @param remoteNodeId
	 * 			  Node's id -- the one responsible 
	 * 			  with the remote bucket (the request's 
	 * 			  destination node)
	 * @param bufferKey
	 * 			  Buffer key -- unique identification
	 * @param sequence
	 * 			  Sequence number (~ chunk number)
	 * @param nrequest
	 * 			  Requests number (how many requests
	 * 			  are sent inside this message)
	 */
	public void handleNewRequest(int idSubmission, int idBucket,
			int remoteNodeId, long bufferKey, int sequence, int nrequest) {
		final TupleInfo tu = tuFactory.get();
		tu.submissionId = idSubmission;
		tu.bucketId = idBucket;
		tu.remoteNodeId = remoteNodeId;
		tu.bucketKey = bufferKey;
		tu.sequence = sequence;
		tu.nrequests = nrequest;

		// Calculate the expected time
		tu.expected = System.currentTimeMillis() + Math.min(1000, 2 * nrequest);

		if (log.isDebugEnabled()) {
			log.debug("TupleRequester insert, node = " + remoteNodeId
					+ ", bucket = " + bufferKey);
		}

		try {
			handleInfo(tu);
		} catch (Throwable e) {
			log.error("Got exception", e);
		}
	}

	/**
	 * Handles (sends) the tuples fetch request to the
	 * destination node, among with related information.
	 * 
	 * @param info
	 * 			  Information regarding the tuple's 
	 * 			  request for transfer
	 */
	private void handleInfo(TupleInfo info) {
		while (true) {
			long currentTime = System.currentTimeMillis();
			
			if (log.isDebugEnabled()) {
				log.debug("currentTime = " + currentTime + ", expected = "
						+ info.expected);
			}
			if (currentTime >= info.expected) {
				synchronized (activeRequests) {
					/*
					 * while (activeRequests.size() >=
					 * Consts.MAX_CONCURRENT_TRANSFERS) { try {
					 * log.debug("Waiting for number of active requests to decrease"
					 * ); activeRequests.wait(); } catch (InterruptedException
					 * e) { // ignore } }
					 */
					activeRequests.add(--ticket);
				}

				try {
					IbisIdentifier id = net.getPeerLocation(info.remoteNodeId);
					WriteMessage msg = net.getMessageToSend(id,
							NetworkLayer.nameMgmtReceiverPort);
					msg.writeByte((byte) 4);
					msg.writeLong(info.bucketKey);
					msg.writeInt(info.submissionId);
					msg.writeInt(info.bucketId);
					msg.writeLong(ticket);
					msg.writeInt(info.sequence);
					msg.writeInt(info.nrequests);
					net.finishMessage(msg, info.submissionId);

					if (log.isDebugEnabled()) {
						log.debug("Sent request to " + id
								+ " to fetch data for bucket " + info.bucketKey
								+ "request=" + info.nrequests + " t="
								+ (System.currentTimeMillis() - currentTime));
					}
					return;
				} catch (Throwable e) {
					log.error("Exception in TupleRequester thread", e);
					removeActiveRequest(ticket);
					return;
				} finally {
					tuFactory.release(info);
				}
			}

			try {
				if (log.isDebugEnabled()) {
					log.debug("Sleeping for " + (info.expected - currentTime)
							+ " milliseconds");
				}
				Thread.sleep(info.expected - currentTime);
			} catch (InterruptedException e) {
				// ignore.
			}
		}
	}

	/**
	 * Removes a pending request from the
	 * waiting queue (active request queue).
	 * 
	 * @param ticket
	 * 			  The identifier of the pending
	 * 			  request
	 */
	public void removeActiveRequest(long ticket) {
		synchronized (activeRequests) {
			activeRequests.remove(ticket);
			/*
			 * if (activeRequests.size() < Consts.MAX_CONCURRENT_TRANSFERS) {
			 * activeRequests.notify(); }
			 */
		}

	}
}
