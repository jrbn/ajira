package arch.actions;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.storage.Factory;

public class ActionsProvider {
    
    	public ActionsProvider() {
    	    
    	}

	static final Logger log = LoggerFactory.getLogger(ActionsProvider.class);

	private Map<String, Factory<Action>> listFactories = new HashMap<String, Factory<Action>>();

	public synchronized void release(Action action) {
		listFactories.get(action.getClass().getName()).release(action);
	}

	public synchronized Action get(String className) {
		try {
			Factory<Action> factory = listFactories.get(className);
			if (factory == null) {
				Class<? extends Action> cRule = ClassLoader
						.getSystemClassLoader().loadClass(className)
						.asSubclass(Action.class);
				factory = new Factory<Action>(cRule);
				listFactories.put(className, factory);
			}
			return factory.get();
		} catch (Exception e) {
			log.error("Class not found", e);
		}

		return null;
	}
}
