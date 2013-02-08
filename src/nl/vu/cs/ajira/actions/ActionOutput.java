package nl.vu.cs.ajira.actions;

import java.util.List;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;

public interface ActionOutput {

	public void output(SimpleData... data) throws Exception;

	public void output(Tuple tuple) throws Exception;

	public void branch(List<ActionConf> actions) throws Exception;

	public void branch(ActionConf action) throws Exception;

	public ActionOutput split(List<ActionConf> actions) throws Exception;

	public ActionOutput split(ActionConf action) throws Exception;
}
