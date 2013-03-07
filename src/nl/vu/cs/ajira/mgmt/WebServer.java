package nl.vu.cs.ajira.mgmt;

import java.net.BindException;
import java.net.InetAddress;

import nl.vu.cs.ajira.Context;

import org.apache.jasper.servlet.JspServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebServer implements Runnable {

	public static final String WEBSERVER_START = "webserver.start";
	public static final String WEBSERVER_PORT = "webserver.port";

	private static final int MAX_ATTEMPTS = 10;

	static final Logger log = LoggerFactory.getLogger(WebServer.class);

	private int serverPort = 8881; // Standard port

	private Context context;
	
	private boolean done = false;
	private boolean failed = false;

	public void startWebServer(Context context) {
		this.context = context;
		serverPort = context.getConfiguration().getInt(WEBSERVER_PORT, 8881);
		Thread thread = new Thread(this, "WebServer");
		thread.start();
	}

	@Override
	public void run() {

		int currentAttempt = 0;
		ServletContextHandler handler = null;
		while (currentAttempt < MAX_ATTEMPTS) {
			try {
				handler = new ServletContextHandler(
						ServletContextHandler.SESSIONS);
				handler.setContextPath("/");
				handler.setClassLoader(Thread.currentThread()
						.getContextClassLoader());

				String mainDir = this.getClass().getClassLoader()
						.getResource("jsp/").toExternalForm();
				handler.setResourceBase(mainDir);

				handler.setAttribute("context", context);
				handler.addServlet(MainServlet.class, "/");
				handler.addServlet(JspServlet.class, "*.jsp");
				Server server = new Server(serverPort);
				server.setHandler(handler);
				server.start();
				synchronized(this) {
					done = true;
					notifyAll();
				}
				server.join();
				return;
			} catch (BindException e) {
				try {
					handler.stop();
				} catch (Exception e1) {
				}
				serverPort++;
				currentAttempt++;
			} catch (Exception e) {
				log.error("The web server instance has terminated", e);
				break;
			}
		}
		synchronized(this) {
			failed = true;
			notifyAll();
		}

	}

	public String getAddress() {
		synchronized(this) {
			while (! done && ! failed) {
				try {
					wait();
				} catch(InterruptedException e) {
					// ignore
				}
			}
		}
		if (failed) {
			return null;
		}
		try {
			return "http://" + InetAddress.getLocalHost().getHostAddress()
					+ ":" + serverPort;
		} catch (Exception e) {
			return "<null>:" + serverPort;
		}
	}
}