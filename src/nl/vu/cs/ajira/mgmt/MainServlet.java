package nl.vu.cs.ajira.mgmt;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainServlet extends HttpServlet {

	private static final long serialVersionUID = 486045906078836674L;

	static final Logger log = LoggerFactory.getLogger(WebServer.class);

	@Override
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {

		if (request.getServletPath().equals("/gc")) {
			long time = System.currentTimeMillis();
			Runtime.getRuntime().gc();
			time = System.currentTimeMillis() - time;
			String msg = "GC completed in " + time + "ms.";
			request.getSession().setAttribute("message", msg);
			response.sendRedirect("/");
		}

		if (request.getServletPath().equals("/")) {
			RequestDispatcher r = request.getRequestDispatcher("main.jsp");
			r.forward(request, response);
		}
	}
}
