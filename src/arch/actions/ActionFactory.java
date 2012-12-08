package arch.actions;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.actions.ActionConf.ParamItem;
import arch.storage.Factory;

public class ActionFactory {

	static final Logger log = LoggerFactory.getLogger(ActionFactory.class);

	private final static Map<String, List<ParamItem>> actionParameters = new ConcurrentHashMap<String, List<ParamItem>>();
	private final Map<String, Factory<Action>> listFactories = new ConcurrentHashMap<String, Factory<Action>>();

	public static ActionConf getActionConf(
			Class<? extends Action> clazz) {
		if (clazz == null)
			return null;

		String className = clazz.getName();
		if (!actionParameters.containsKey(className)) {
			// Setup the list of parameters
			List<ParamItem> params = null;
			try {
				Class<? extends Action> a = Class.forName(className)
						.asSubclass(Action.class);
				Action action = a.newInstance();
				ActionConf conf = new ActionConf(className, null);
				action.setupActionParameters(conf);
				params = conf.getListAllowedParameters();
			} catch (Exception e) {
				log.error("Failed in retrieving the parameter list for class"
						+ className);
			}
			actionParameters.put(className, params);
		}

		return new ActionConf(className, actionParameters.get(clazz.getName()));
	}

	public Action getAction(String className, DataInput rawParams)
			throws IOException {

		Object[] params = ActionConf.readValuesFromStream(rawParams);

		try {
			Factory<Action> factory = listFactories.get(className);
			if (factory == null) {
				synchronized (this) {
					if (!listFactories.containsKey(className)) {
						Class<? extends Action> cRule = ClassLoader
								.getSystemClassLoader().loadClass(className)
								.asSubclass(Action.class);
						factory = new Factory<Action>(cRule);
						listFactories.put(className, factory);
					}
				}
			}
			Action a = factory.get();
			a.setParams(params);
			return a;
		} catch (Exception e) {
			log.error("Class not found", e);
		}

		return null;
	}

	public void release(Action action) {
		action.setParams(null);
		listFactories.get(action.getClass().getName()).release(action);
	}
}