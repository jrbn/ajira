package nl.vu.cs.ajira.webinterface;

import java.net.BindException;

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

	private int serverPort = 8080; // Standard port

	private Context context;

	public void startWebServer(Context context) {
		this.context = context;
		serverPort = context.getConfiguration().getInt(WEBSERVER_PORT, 8080);
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
				handler.setResourceBase(System.getProperty("user.dir"));
				handler.setAttribute("context", context);

				handler.addServlet(MainServlet.class, "/");
				handler.addServlet(JspServlet.class, "*.jsp");
				Server server = new Server(serverPort);
				server.setHandler(handler);
				server.start();
				server.join();
				break;
			} catch (BindException e) {
				try {
					handler.stop();
				} catch (Exception e1) {
				}
				serverPort++;
				currentAttempt++;
			} catch (Exception e) {
				log.error("The web server instance has terminated", e);
				return;
			}
		}

	}
}