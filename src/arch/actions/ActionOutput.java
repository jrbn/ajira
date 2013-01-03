package arch.actions;

import java.util.List;

import arch.data.types.Tuple;

public interface ActionOutput {

	public void output(Tuple tuple) throws Exception;

	public boolean isBranchingAllowed();

	public void branch(List<ActionConf> actions) throws Exception;

	public void branch(ActionConf action) throws Exception;
}
