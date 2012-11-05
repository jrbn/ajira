package arch.chains;

import arch.data.types.Tuple;

public interface ChainContinuation {
    public boolean add(Tuple element) throws Exception;
}
