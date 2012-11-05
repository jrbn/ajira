package arch.utils;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration extends Properties {

    static final Logger log = LoggerFactory.getLogger(Configuration.class);

    private static final long serialVersionUID = 1L;

    public Configuration() {
	// Copy all the system properties in it
	/*
	 * for (java.util.Map.Entry<Object, Object> value :
	 * System.getProperties() .entrySet()) { put(value.getKey(),
	 * value.getValue()); }
	 */
    }

    public int getInt(String prop, int defaultValue) {
	try {
	    String value = this.getProperty(prop);
	    return Integer.valueOf(value);
	} catch (Exception e) {
	}

	return defaultValue;
    }

    public void setInt(String prop, int value) {
	setProperty(prop, Integer.toString(value));
    }

    public void set(String prop, String value) {
	setProperty(prop, value);
    }

    public String get(String prop, String defaultValue) {
	String value = getProperty(prop);
	if (value == null) {
	    return defaultValue;
	}

	return value;
    }

    public boolean getBoolean(String prop, boolean defaultValue) {
	try {
	    return getProperty(prop).equalsIgnoreCase("true");
	} catch (Exception e) {
	}

	return defaultValue;
    }

    public void setBoolean(String prop, boolean value) {
	setProperty(prop, Boolean.toString(value));
    }
}