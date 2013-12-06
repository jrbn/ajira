package nl.vu.cs.ajira.utils;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to add configuration properties to the cluster and get
 * this properties.
 * 
 */
public class Configuration extends Properties {

	static final Logger log = LoggerFactory.getLogger(Configuration.class);

	private static final long serialVersionUID = 1L;

	/**
	 * 
	 * @param prop
	 *            The key (property name) of the value that is looked.
	 * @param defaultValue
	 *            The default value that is returned if the key does not exists.
	 * @return The value converted to Integer of the key prop.
	 */
	public int getInt(String prop, int defaultValue) {
		String value = this.getProperty(prop);
		if (value == null) {
			value = System.getProperty(prop);
			if (value == null) {
				return defaultValue;
			}
		}
		try {
			return Integer.valueOf(value);
		} catch(NumberFormatException e) {
			log.warn("Got exception", e);
			return defaultValue;
		}
	}

	/**
	 * Add to the property list the key prop and its corresponding value
	 * converted to String.
	 * 
	 * @param prop
	 *            The key (property name) that is added in the property list.
	 * @param value
	 *            The value of the key prop.
	 */
	public void setInt(String prop, int value) {
		setProperty(prop, Integer.toString(value));
	}

	/**
	 * Add to the property list the key prop and its corresponding value.
	 * 
	 * @param prop
	 *            The key (property name) that is added in the property list.
	 * @param value
	 *            The value of the key prop.
	 */
	public void set(String prop, String value) {
		setProperty(prop, value);
	}

	/**
	 * 
	 * @param prop
	 *            The key (property name) of the value that is looked.
	 * @param defaultValue
	 *            The default value that is returned if the key does not exists.
	 * @return The value of the key prop or the defaultValue if the key prop
	 *         does not exists.
	 */
	public String get(String prop, String defaultValue) {
		String value = getProperty(prop);
		if (value == null) {
			value = System.getProperty(prop);
			if (value == null) {
				return defaultValue;
			}
		}

		return value;
	}

	/**
	 * 
	 * @param prop
	 *            The key (property name) of the required property.
	 * @param defaultValue
	 *            The default value that is returned if the key does not exist.
	 * @return The value of the key prop or the defaultValue if the key prop
	 *         does not exist.
	 */
	public boolean getBoolean(String prop, boolean defaultValue) {
		String value = this.getProperty(prop);
		if (value == null) {
			value = System.getProperty(prop);
		}
		if (value != null) {
			return value.equals("true");
		}
		return defaultValue;
	}

	/**
	 * Add to the property list the key prop and its corresponding value.
	 * 
	 * @param prop
	 *            The key (property name) that is added in the property list.
	 * @param value
	 *            he value of the key prop.
	 */
	public void setBoolean(String prop, boolean value) {
		setProperty(prop, Boolean.toString(value));
	}
}