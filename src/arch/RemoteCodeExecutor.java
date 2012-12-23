package arch;

import arch.actions.ActionContext;

public interface RemoteCodeExecutor {
    public void execute(ActionContext context) throws Exception;
}
