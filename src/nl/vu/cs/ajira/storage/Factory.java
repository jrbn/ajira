package nl.vu.cs.ajira.storage;

import java.lang.reflect.Constructor;

import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Factory<K> {

    static final Logger log = LoggerFactory.getLogger(Factory.class);

    Object[] buffer = new Object[Consts.N_ELEMENTS_FACTORY];
    int bufferSize = 0;

    Constructor<? extends K> constructor;
    Object[] params;

    public Factory(Class<? extends K> class1, Object... params) {
	try {
	    this.params = params;
	    if (params != null && params.length > 0) {
		Class<?>[] clazzes = new Class<?>[params.length];
		int i = 0;
		for (Object param : params) {
		    clazzes[i++] = param.getClass();
		}
		constructor = class1.getConstructor(clazzes);
	    } else {
		constructor = class1.getConstructor();
	    }
	} catch (Exception e) {
	    log.error("Error initializing factory", e);
	}
    }

    // int ngenerated = 0;

    @SuppressWarnings("unchecked")
    public synchronized K get() {
	try {
	    if (bufferSize == 0) {
		K el = constructor.newInstance(params);
		//
		// ngenerated++;
		// log.debug("new " + constructor.getName());
		// + " element (ngenerated=" + ngenerated + ")");

		return el;
	    } else {
		bufferSize--;
		K el = (K) buffer[bufferSize];
		buffer[bufferSize] = null; // Set to null, so we can leak ...
					   // --Ceriel
		return el;
	    }
	} catch (Exception e) {
	    log.error("Error in instantiation", e);
	}

	return null;
    }

    public synchronized boolean release(K element) {
	// if (print)
	// log.error("Release " + element);
	if (bufferSize < buffer.length) {
	    buffer[bufferSize++] = element;
	    return true;
	}
	log.warn("Factory is too small. Throwing away stuff");
	return false;
    }

    public int getMaxCapacity() {
	return buffer.length;
    }

    public void setMaxCapacity(int max) {
	buffer = new Object[max];
	bufferSize = 0;
    }
}
