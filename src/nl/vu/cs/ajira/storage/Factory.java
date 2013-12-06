package nl.vu.cs.ajira.storage;

import java.lang.reflect.Constructor;
import java.util.Random;

import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Factory<K> {

	static final Logger log = LoggerFactory.getLogger(Factory.class);

	private Object[] buffer = new Object[Consts.N_ELEMENTS_FACTORY];
	private int bufferSize = 0;

	private Constructor<? extends K> constructor;
	private Object[] params;

	/**
	 * Construct a new Factory and sets the constructor and params field of the
	 * class.
	 * 
	 * @param class1
	 * @param params
	 *            is a array or a sequence of Objects that are needed for
	 *            class1's constructor
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
		} catch (Throwable e) {
			log.error("Error initializing factory", e);
			throw new Error("Internal error initializing factory", e);
		}
	}

	public synchronized int getNAvailableElements() {
		return bufferSize;
	}

	public synchronized long getEstimatedSizeInBytes(Random r) {
		// Get 10 random elements and estimate the size

		long estimatedSize = 0;
		if (getNAvailableElements() < Consts.N_ELEMENTS_TO_SAMPLE) {
			// Count them all
			for (int i = 0; i < bufferSize; ++i) {
				Object el = buffer[i];
				if (el instanceof WritableContainer) {
					estimatedSize += ((WritableContainer<?>) el)
							.getTotalCapacity();
				}
			}
			return estimatedSize;
		} else {
			for (int i = 0; i < Consts.N_ELEMENTS_TO_SAMPLE; ++i) {
				Object el = buffer[r.nextInt(bufferSize)];
				if (el instanceof WritableContainer) {
					estimatedSize += ((WritableContainer<?>) el)
							.getTotalCapacity();
				}
			}
			return estimatedSize / Consts.N_ELEMENTS_TO_SAMPLE * bufferSize;
		}
	}

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
		} catch (Throwable e) {
			log.error("Error in instantiation", e);
			throw new Error("Internal error in instantiation", e);
		}
	}

	/**
	 * Adds the element at the end of the buffer if there is space.
	 * 
	 * @param element
	 *            is the elements that is added to the buffer
	 * @return true if there is space to add the element false if there is not
	 *         space to add the element
	 */
	public synchronized boolean release(K element) {
		// if (print)
		// log.error("Release " + element);
		if (bufferSize < buffer.length) {
			buffer[bufferSize++] = element;
			return true;
		}
		if (log.isInfoEnabled()) {
			log.info("Factory is too small. Throwing away stuff");
		}
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
	 * Sets the buffer to a new array with the capacity max and resets the
	 * bufferSize.
	 * 
	 * @param max
	 *            is the new capacity of the buffer
	 */
	public void setMaxCapacity(int max) {
		buffer = new Object[max];
		bufferSize = 0;
	}

	/**
	 * Cleans the factory to free memory
	 * 
	 */
	public void clean() {
		bufferSize = 0;
		buffer = new Object[Consts.N_ELEMENTS_FACTORY];
	}
}
