package nl.vu.cs.ajira.actions;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import nl.vu.cs.ajira.actions.ActionConf.Configurator;
import nl.vu.cs.ajira.actions.ActionConf.ParamItem;
import nl.vu.cs.ajira.storage.Factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ActionFactory {

	private static class ParamsInfo {
		List<ParamItem> params;
		Configurator proc;
	}

	static final Logger log = LoggerFactory.getLogger(ActionFactory.class);

	private final static Map<String, ParamsInfo> actionParameters = new ConcurrentHashMap<String, ParamsInfo>();
	private final Map<String, Factory<Action>> listFactories = new ConcurrentHashMap<String, Factory<Action>>();

	public static ActionConf getActionConf(String className) {
		if (!actionParameters.containsKey(className)) {
			// Setup the list of parameters
			try {
				Class<? extends Action> a = Class.forName(className)
						.asSubclass(Action.class);
				Action action = a.newInstance();
				ActionConf conf = new ActionConf(className);
				action.registerActionParameters(conf);

				ParamsInfo info = new ParamsInfo();
				info.params = conf.getListAllowedParameters();
				if (info.params == null) {
					info.params = new ArrayList<ParamItem>();
				}
				info.proc = conf.getConfigurator();
				actionParameters.put(className, info);
			} catch (Exception e) {
				log.error("Failed in retrieving the parameter list for class"
						+ className);
			}
		}

		ParamsInfo i = actionParameters.get(className);
		return new ActionConf(className, i.params, i.proc);
	}

	public static ActionConf getActionConf(Class<? extends Action> clazz) {
		if (clazz == null)
			return null;
		return getActionConf(clazz.getName());
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
					} else {
						factory = listFactories.get(className);
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
