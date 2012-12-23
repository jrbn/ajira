package arch.actions;

import java.util.List;

import arch.data.types.Tuple;

public interface Output {

	public void output(Tuple tuple) throws Exception;

	public boolean isBranchingAllowed();

	public void branch(List<ActionConf> actions) throws Exception;

}
