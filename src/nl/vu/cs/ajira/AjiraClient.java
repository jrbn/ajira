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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import nl.vu.cs.ajira.buckets.WritableTuple;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.exceptions.JobFailedException;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.net.ReadMessageWrapper;
import nl.vu.cs.ajira.net.WriteMessageWrapper;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client for an Ajira cluster.
 */
public class AjiraClient {

	static final Logger log = LoggerFactory.getLogger(AjiraClient.class);

	private final Ibis ibis;
	private final IbisIdentifier server;
	private final ReceivePort rp;
	private final SendPort sp;
	private boolean closed = false;
	private boolean getResultCalled = false;
	private long bucketKey = -1L;
	private double time;

	/**
	 * Constructs an Ajira client from an Ajira cluster described by the
	 * specified properties file, and submits a job to the cluster.
	 * 
	 * @param filename
	 *            the cluster properties.
	 * @param job
	 *            the job to submit.
	 * @throws IOException
	 *             is thrown when the client creation fails for some reason.
	 */
	public AjiraClient(String filename, Job job) throws IOException {

		Properties properties = new Properties();

		if (filename != null && new File(filename).exists()) {
			properties.load(new BufferedInputStream(new FileInputStream(
					filename)));
		}

		// Create an ibis.
		try {
			ibis = IbisFactory.createIbis(NetworkLayer.ibisServerCapabilities,
					properties, true, null, NetworkLayer.mgmtRequestPortType,
					NetworkLayer.queryAnswerPortType);
		} catch (IbisCreationFailedException e) {
			throw new IOException("Could not create Ibis", e);
		}

		// Determine which node to connect to, and create a receive port to
		// receive
		// answers from the cluster.
		try {
			server = ibis.registry().getElectionResult("server");
			rp = ibis.createReceivePort(NetworkLayer.queryAnswerPortType,
					NetworkLayer.queryReceiverPort);
		} catch (IOException e) {
			log.error("Could not get server", e);
			close();
			throw e;
		}

		rp.enableConnections();

		// Create a sendport, connect, and write message.
		WriteMessage msg = null;
		try {
			sp = ibis.createSendPort(NetworkLayer.mgmtRequestPortType);
			sp.connect(server, NetworkLayer.queryReceiverPort);
			msg = sp.newMessage();
			msg.writeByte((byte) 7);
			job.writeTo(new WriteMessageWrapper(msg));
			msg.finish();
		} catch (IOException e) {
			log.error("Problem sending job", e);
			if (msg != null) {
				msg.finish(e);
			}
			close();
			throw e;
		}
	}

	public boolean getResult(ArrayList<Tuple> v) throws JobFailedException {

		ReadMessage r;

		if (getResultCalled) {
			log.error("Already called getResult");
			throw new Error("Already called getresult");
		}
		getResultCalled = true;
		try {
			r = rp.receive();
			boolean success = r.readBoolean();
			if (!success) {
				try {
					int len = r.readInt();
					byte[] b = new byte[len];
					r.readArray(b);
					r.finish();
					ByteArrayInputStream bi = new ByteArrayInputStream(b);
					ObjectInputStream i = new ObjectInputStream(bi);
					JobFailedException e;
					try {
						e = (JobFailedException) i.readObject();
					} catch (ClassNotFoundException e1) {
						throw new JobFailedException(
								"Job failed, but could not obtain the reason",
								e1);
					}
					throw e;
				} finally {
					close();
				}
			}
			time = r.readDouble();
			WritableContainer<WritableTuple> tmpBuffer = new WritableContainer<WritableTuple>(
					Consts.TUPLES_CONTAINER_MAX_BUFFER_SIZE);
			tmpBuffer.readFrom(new ReadMessageWrapper(r));

			int lengthTuples = r.readInt();
			byte[] signature = new byte[lengthTuples];
			for (int i = 0; i < lengthTuples; ++i) {
				signature[i] = r.readByte();
			}

			boolean isFinished = r.readBoolean();
			if (!isFinished) {
				bucketKey = r.readLong();
			}
			r.finish();

			v.clear();
			if (tmpBuffer.getNElements() > 0) {
				WritableTuple ts = new WritableTuple();
				Tuple t = TupleFactory.newTuple(DataProvider.get().getArrayOf(
						signature));
				ts.setTuple(t);
				while (tmpBuffer.remove(ts)) {
					v.add(t);
					t = TupleFactory.newTuple(DataProvider.get().getArrayOf(
							signature));
					ts.setTuple(t);
				}
			}

			if (isFinished) {
				close();
			}
			return isFinished;
		} catch (IOException e) {
			close();
			throw new JobFailedException("Could not read result", e);
		}
	}

	public double getTime() {
		if (!getResultCalled) {
			log.error("getResult() not called yet, time is not available");
			return 0.0;
		}
		return time;
	}

	public boolean getMoreResults(List<Tuple> v) throws JobFailedException {
		if (closed || !getResultCalled) {
			log.error("getMoreResults called incorrectly");
			throw new JobFailedException("getMoreResults called incorrectly",
					null);
		}
		try {
			WriteMessage msg = sp.newMessage();
			msg.writeByte((byte) 9);
			msg.writeLong(bucketKey);
			msg.finish();

			WritableContainer<WritableTuple> tmpBuffer = new WritableContainer<WritableTuple>(
					Consts.MAX_SIZE);
			ReadMessage r = rp.receive();
			tmpBuffer.readFrom(new ReadMessageWrapper(r));

			int lengthTuples = r.readInt();
			byte[] signature = new byte[lengthTuples];
			for (int i = 0; i < lengthTuples; ++i) {
				signature[i] = r.readByte();
			}

			boolean isFinished = r.readBoolean();
			r.finish();
			v.clear();

			WritableTuple ts = new WritableTuple();
			Tuple t = TupleFactory.newTuple(DataProvider.get().getArrayOf(
					signature));
			ts.setTuple(t);
			while (tmpBuffer.remove(ts)) {
				v.add(t);
				t = TupleFactory.newTuple(DataProvider.get().getArrayOf(
						signature));
				ts.setTuple(t);
			}
			if (isFinished) {
				close();
			}
			return isFinished;
		} catch (IOException e) {
			close();
			throw new JobFailedException("Could not read result", e);
		}
	}

	@Override
	protected void finalize() {
		close();
	}

	private void close() {
		if (sp != null) {
			try {
				sp.close();
			} catch (Throwable e) {
				// ignore
			}
		}
		if (rp != null) {
			try {
				rp.close(-1);
			} catch (Throwable e) {
				// ignore
			}
		}
		try {
			ibis.end();
		} catch (Throwable e) {
			// ignore
		}
		closed = true;
	}
}
