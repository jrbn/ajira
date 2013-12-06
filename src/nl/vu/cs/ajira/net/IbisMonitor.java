package nl.vu.cs.ajira.net;

import ibis.ipl.Ibis;

import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IbisMonitor implements Runnable {

	private static final int INTERVAL = 1000;

	static final Logger log = LoggerFactory.getLogger(IbisMonitor.class);

	private ObjectName operatingSystemBean;
	private MBeanServer beanServer;
	private Data oldData = null;
	private final Ibis ibis;

	private boolean monitoring;

	private static class Data {
		public long sent;
		public long rcvd;
		public long cputm;
		public long time;
	}

	/**
	 * Creates a new IbisMonitor.
	 * 
	 * @param ibis
	 *            Is a Ibis instance.
	 * @return A newly created IbisMonitor.
	 * @throws Exception
	 */
	public static IbisMonitor createMonitor(Ibis ibis) throws Exception {
		if (!log.isDebugEnabled()) {
			return null;
		}
		return new IbisMonitor(ibis);
	}

	/**
	 * Custom constructor. Sets the ibis field of the class and starts a daemon
	 * thread.
	 * 
	 * @param ibis
	 *            The new instance of Ibis.
	 * @throws Exception
	 */
	private IbisMonitor(Ibis ibis) throws Exception {
		this.ibis = ibis;

		if (!log.isDebugEnabled()) {
			return;
		}

		beanServer = java.lang.management.ManagementFactory
				.getPlatformMBeanServer();
		operatingSystemBean = new ObjectName("java.lang:type=OperatingSystem");

		Thread p = new Thread(this);
		p.setDaemon(true);
		p.setName("IbisMonitor thread");
		p.start();
	}

	private Object getCpuTime() throws Exception {
		return beanServer.getAttribute(operatingSystemBean, "ProcessCpuTime");
	}

	/**
	 * It updates the properties and puts the informations into log.
	 * 
	 * @param num
	 *            It reflect how many times the INTERVAL was added at the
	 *            waiting time.
	 * @throws Exception
	 */
	private void logData(int num) throws Exception {
		Map<String, String> properties = ibis.managementProperties();
		long sent = Long.parseLong(properties.get("bytesWritten"));
		long read = Long.parseLong(properties.get("bytesReceived"));
		long cputime = Long.parseLong(getCpuTime().toString());

		long time = System.currentTimeMillis();
		double factor = 1000.0 / (time - oldData.time);
		if (log.isInfoEnabled()) {
			log.info("[num, cpu time, bytes sent, bytes read] = [" + num + ", "
					+ ((cputime - oldData.cputm) / 1.0e9) * factor + ", "
					+ (sent - oldData.sent) + ", " + (read - oldData.rcvd)
					+ "]");
		}
		oldData.sent = sent;
		oldData.rcvd = read;
		oldData.cputm = cputime;
		oldData.time = time;
	}

	/**
	 * Sets if the ibis is monitored. If it is true it wakes up the thread.
	 * 
	 * @param value
	 *            The new value of monitoring.
	 */
	public synchronized void setMonitoring(boolean value) {
		if (monitoring != value) {
			monitoring = value;
			if (value) {
				notifyAll();
			}
		}
	}

	/**
	 * If the monitoring variable is true it logs the attributes of the Ibis
	 * instance from time to time.
	 */
	@Override
	public synchronized void run() {
		for (;;) {
			while (!monitoring) {
				try {
					wait();
				} catch (InterruptedException e) {
					// ignore
				}
			}
			int statsCount = 0;
			long time = System.currentTimeMillis();
			oldData = new Data();
			oldData.time = time;
			Map<String, String> properties = ibis.managementProperties();
			oldData.sent = Long.parseLong(properties.get("bytesWritten"));
			oldData.rcvd = Long.parseLong(properties.get("bytesReceived"));
			try {
				oldData.cputm = Long.parseLong(getCpuTime().toString());
			} catch (Throwable e) {
				log.warn("Exception caught in logData", e);
				return;
			}
			while (monitoring) {
				statsCount++;
				long waitTime = statsCount * INTERVAL + time
						- System.currentTimeMillis();
				while (waitTime < 0) {
					statsCount++;
					waitTime += INTERVAL;
				}
				if (waitTime > 0) {
					try {
						wait(waitTime);
					} catch (InterruptedException e) {
						// ignore
					}
				}
				try {
					logData(statsCount);
				} catch (Throwable e) {
					log.warn("Exception caught in logData", e);
					return;
				}
			}
		}
	}
}
