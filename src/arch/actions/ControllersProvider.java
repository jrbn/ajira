package arch.actions;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.storage.Factory;

public class ControllersProvider {

	public ControllersProvider() {

	}

	static final Logger log = LoggerFactory
			.getLogger(ControllersProvider.class);

	private final Map<String, Factory<Controller>> listFactories = new HashMap<String, Factory<Controller>>();

	public synchronized void release(Controller action) {
		listFactories.get(action.getClass().getName()).release(action);
	}

	public synchronized Controller get(String className) {
		try {
			Factory<Controller> factory = listFactories.get(className);
			if (factory == null) {
				Class<? extends Controller> cRule = ClassLoader
						.getSystemClassLoader().loadClass(className)
						.asSubclass(Controller.class);
				factory = new Factory<Controller>(cRule);
				listFactories.put(className, factory);
			}
			return factory.get();
		} catch (Exception e) {
			log.error("Class not found", e);
		}

		return null;
	}
}
