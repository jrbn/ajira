package arch.actions;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public abstract class Controller {

	public abstract Chain apply(ActionContext context, Tuple tuple,
			Chain chain, WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToSend) throws Exception;

}
