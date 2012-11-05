package arch.webinterface;

import org.apache.jasper.servlet.JspServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.Context;

public class WebServer implements Runnable {

	public static final String WEBSERVER_START = "webserver.start";
	public static final String WEBSERVER_PORT = "webserver.port";

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
		Server server = new Server(serverPort);

		ServletContextHandler handler = new ServletContextHandler(
				ServletContextHandler.SESSIONS);
		handler.setContextPath("/");
		handler.setClassLoader(Thread.currentThread().getContextClassLoader());
		handler.setResourceBase(System.getProperty("user.dir"));
		handler.setAttribute("context", context);

		handler.addServlet(MainServlet.class, "/");
		handler.addServlet(JspServlet.class, "*.jsp");

		try {
			server.setHandler(handler);
			server.start();
			server.join();
		} catch (Exception e) {
			log.error("The web server instance has terminated", e);
		}
	}
}