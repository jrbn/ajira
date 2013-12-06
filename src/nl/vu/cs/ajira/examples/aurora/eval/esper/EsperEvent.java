package nl.vu.cs.ajira.examples.aurora.eval.esper;

import java.util.Map;

public class EsperEvent {
	public String type;
	public Map<String, Object> attributes;
	
	EsperEvent(String type, Map<String, Object> attributes) {
		this.type = type;
		this.attributes = attributes;
	}
}
