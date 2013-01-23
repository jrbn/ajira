package nl.vu.cs.ajira.actions;

import java.util.List;

import nl.vu.cs.ajira.data.types.Tuple;


public interface ActionOutput {

	public void output(Tuple tuple) throws Exception;

	public boolean isRootBranch();

	public void branch(List<ActionConf> actions) throws Exception;

	public void branch(ActionConf action) throws Exception;
}
