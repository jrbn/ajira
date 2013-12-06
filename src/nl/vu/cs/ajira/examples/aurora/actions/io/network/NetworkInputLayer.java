package nl.vu.cs.ajira.examples.aurora.actions.io.network;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.Location;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.utils.Configuration;

public class NetworkInputLayer extends InputLayer {
	private static final String ACCEPT_PORT = "__NetworkInputLayer_AcceptPort";
	private ServerSocket serverSocket;
	private Socket socket;
	private int port;

	public static void setAcceptPort(Configuration conf, int port) {
		conf.setInt(ACCEPT_PORT, port);
	}

	@Override
	protected void load(Context context) throws Exception {
		port = context.getConfiguration().getInt(ACCEPT_PORT, 9000);
		try {
			serverSocket = new ServerSocket(port);
			socket = serverSocket.accept();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		NetworkTupleIterator itr = null;
		try {
			itr = new NetworkTupleIterator(socket);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return itr;
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
		// Nothing to do
	}

	@Override
	public Location getLocations(Tuple tuple, ActionContext context) {
		// For now it supports only a local machine.
		return Location.THIS_NODE;
	}

	@Override
	public void close() {
		try {
			serverSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
