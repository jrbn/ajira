<%@page import="nl.vu.cs.ajira.chains.ChainResolver"%>
<%@page import="nl.vu.cs.ajira.chains.ChainHandler"%>
<%@page import="java.util.List"%>
<%@page import="nl.vu.cs.ajira.utils.Consts"%>
<%@page import="nl.vu.cs.ajira.net.NetworkLayer"%>
<%@page import="arch.Context"%>
<%@page import="arch.Arch"%>
<html>

<%
	Context context = (Context) request.getServletContext()
			.getAttribute("context");
	NetworkLayer net = context.getNetworkLayer();
	String message = (String) request.getSession().getAttribute(
			"message");

	//Get active chain handlers
	int activeChainHandlers = 0;
	for (ChainHandler handler : context.getListChainHandlers()) {
		if (handler.active) {
			activeChainHandlers++;
		}
	}

	//Get active chain resolvers
	int activeChainResolvers = 0;
	for (ChainResolver handler : context.getListChainResolvers()) {
		if (handler.active) {
			activeChainResolvers++;
		}
	}
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
				<td><b># Active Chain Resolvers</b></td>
				<td class="values"><%=activeChainResolvers%> / <%=context.getConfiguration()
					.getInt(Consts.N_RES_THREADS, 1)%></td>
			</tr>
			<tr>
				<td><b># Active Chain Handlers</b></td>
				<td class="values"><%=activeChainHandlers%> / <%=context.getConfiguration().getInt(Consts.N_PROC_THREADS,
					1)%></td>
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
				<td><b># Elements in ChainRes. Buffer</b></td>
				<td class="values"><%=context.getChainsToResolve().getNElements()%></td>
			</tr>
			<tr>
				<td><b>Size of ChainRes. Buffer</b></td>
				<td class="values"><%=context.getChainsToResolve().getRawElementsSize() / 1024 / 1024%>
					/ <%=context.getChainsToResolve().inmemory_size() / 1024%> KB</td>
			</tr>
			<tr>
				<td><b># Elements in ChainHandl. Buffer</b></td>
				<td class="values"><%=context.getChainsToProcess().getNElements()%></td>
			</tr>
			<tr>
				<td><b>Size of ChainHandl. Buffer</b></td>
				<td class="values"><%=context.getChainsToProcess().getRawElementsSize() / 1024 / 1024%>
					/ <%=context.getChainsToProcess().inmemory_size() / 1024%> KB</td>
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