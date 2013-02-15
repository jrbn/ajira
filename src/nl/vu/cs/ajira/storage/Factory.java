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

    /**
     * Construct a new Factory and sets the constructor and params field of the class.
     * @param class1 
     * @param params is a array or a sequence of Objects that
     * 		  are needed for class1's constructor 
     */
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
    /**
     * 
     * @return the Object from the buffer found at the position bufferSize.
     */
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

    /**
     * Adds the element at the end of the buffer if there is space.
     * @param element is the elements that is added to the buffer
     * @return true if there is space to add the element
     * 		   false if there is not space to add the element	
     */
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

    /**
     * 
     * @return the maximum capacity of the buffer
     */
    public int getMaxCapacity() {
    	return buffer.length;
    }

    /**
     * Sets the buffer to a new array with the capacity max and resets the bufferSize.
     * @param max is the new capacity of the buffer
     */
    public void setMaxCapacity(int max) {
		buffer = new Object[max];
		bufferSize = 0;
    }
}
