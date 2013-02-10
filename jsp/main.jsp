<%@page import="nl.vu.cs.ajira.chains.ChainHandlerManager"%>
<%@page import="nl.vu.cs.ajira.Context"%>
<%@page import="nl.vu.cs.ajira.chains.ChainHandler"%>
<%@page import="java.util.List"%>
<%@page import="nl.vu.cs.ajira.utils.Consts"%>
<%@page import="nl.vu.cs.ajira.net.NetworkLayer"%>
<html>

<%
	Context context = (Context) request.getServletContext()
			.getAttribute("context");
	NetworkLayer net = context.getNetworkLayer();
	String message = (String) request.getSession().getAttribute(
			"message");

	//Get active chain handlers
	ChainHandlerManager manager = context.getChainHandlerManager(); 
	int activeChainHandlers = manager.getActiveChainHandlers();
	int inactiveChainHandlers = manager.getInactiveChainHandlers();
	int waitChainHandlers = manager.getWaitChainHandlers();
%>

<head>
<title>Monitor Performance</title>
<style type="text/css">
td {
	padding-left: 5px;
	padding-right: 5px;
}

.values {
	text-align: center;
}

div {
	float: left;
	margin-left: 10px;
}
</style>
</head>

<body>

	<h1>Performance Monitor</h1>

	<%
		if (message != null && message.length() > 0) {
	%>
	<h3><%=message%></h3>

	<%
		request.getSession().setAttribute("message", "");
		}
	%>

	<div>
		<h2>Execution Details</h2>
		<table border="1">			
			<tr>
				<td><b># Active Chain Handlers</b></td>
				<td class="values"><%=activeChainHandlers%></td>
			</tr>
			<tr>
				<td><b># Inactive Chain Handlers</b></td>
				<td class="values"><%=inactiveChainHandlers%></td>
			</tr>
			<tr>
				<td><b># Waiting Chain Handlers</b></td>
				<td class="values"><%=waitChainHandlers%></td>
			</tr>
			<tr>
				<td><b># Active File Mergers</b></td>
				<td class="values"><%=context.getMergeSortThreadsInfo().activeThreads%>
					/ <%=context.getMergeSortThreadsInfo().threads%></td>
			</tr>			
			<tr>
				<td><b># Active Chain Sender</b></td>
				<td class="values"></td>
			</tr>
			<tr>
				<td><b># Active Tuple Sender</b></td>
				<td class="values"></td>
			</tr>
			<tr>
				<td><b># Active Chain Terminator</b></td>
				<td class="values"></td>
			</tr>
		</table>
	</div>

	<div>
		<h2>Memory Management</h2>
		<table border="1">
			<tr>
				<td><b>Heap Mem. Used</b></td>
				<td class="values"><%=Runtime.getRuntime().totalMemory() / 1024 / 1024%>
					/ <%=Runtime.getRuntime().maxMemory() / 1024 / 1024%> MB (<a
					href="/gc">Launch GC</a>)</td>
			</tr>			
			<tr>
				<td><b># Chains still to process</b></td>
				<td class="values"><%=manager.getChainsToProcess().getNElements()%></td>
			</tr>
			<tr>
				<td><b>Size of Chains Buffer</b></td>
				<td class="values"><%=manager.getChainsToProcess().getRawSize() / 1024 / 1024%>
					/ <%=manager.getChainsToProcess().getTotalCapacity() / 1024%> KB</td>
			</tr>
		</table>
	</div>

	<%
		if (net.getNumberNodes() > 1) {
	%>
	<div>
		<h2>Cluster Details</h2>
		<table border="1">
			<tr>
				<td><b>Total # of nodes</b></td>
				<td class="values"><%=net.getNumberNodes()%></td>
			</tr>

			<tr>
				<td><b>My partition</b></td>
				<td class="values"><%=net.getMyPartition()%></td>
			</tr>
			<tr>
				<td><b>Am I the master?</b></td>
				<td class="values"><%=net.isServer()%></td>
			</tr>
			<tr>
				<td><b>Address Master Node</b></td>
				<td class="values"><%=net.getServer()%></td>
			</tr>

			<tr>
				<td><b>List Other Nodes</b></td>
				<td class="values">
					<ul>
						<%
							for (int i = 0; i < net.getNumberNodes(); ++i) {
									if (i != net.getMyPartition()) {
						%>
						<li><%=i%>=<%=net.getPeerLocation(i)%></li>
						<%
							}
								}
						%>
					</ul>
				</td>
			</tr>
		</table>
	</div>
	<%
		}
	%>

	<div>
		<h2>Generic Info</h2>
		<table border="1">
			<tr>
				<td><b>Host</b></td>
				<td class="values"><%=request.getServerName()%></td>
			</tr>
			<tr>
				<td><b>Available Processors</b></td>
				<td class="values"><%=Runtime.getRuntime().availableProcessors()%></td>
			</tr>
			<tr>
				<td><b>Oper. Sys. Info</b></td>
				<td class="values"><%=System.getProperty("os.name")%> <%=System.getProperty("os.version")%>
					<%=System.getProperty("os.arch")%></td>
			</tr>
			<tr>
				<td><b>Java Version</b></td>
				<td class="values"><%=System.getProperty("java.version")%></td>
			</tr>
		</table>
	</div>

</body>

</html>