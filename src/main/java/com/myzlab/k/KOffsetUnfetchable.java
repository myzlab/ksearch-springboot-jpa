package com.myzlab.k;

import static com.myzlab.k.KFunction.*;
import java.util.List;

public class KOffsetUnfetchable extends KQuery {
    
    private KOffsetUnfetchable() {
        super();
    }
    
    private KOffsetUnfetchable(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final long start
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
        
        this.process(start);
    }
    
    private KOffsetUnfetchable(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
    }
    
    protected static KOffsetUnfetchable getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final long start
    ) {
        return new KOffsetUnfetchable(kExecutor, kSpecialFunctions, kQueryData, start);
    }
    
    protected static KOffsetUnfetchable getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData
    ) {
        return new KOffsetUnfetchable(kExecutor, kSpecialFunctions, kQueryData);
    }
    
    public KTable as(
        final String alias
    ) {
        return table(this, alias);
    }
    
    public KTable as(
        final String alias,
        final String... names
    ) {
        return table(this, alias, tuple(names));
    }
    
    private void process(
        final long start
    ) {
        KQueryUtils.processOffset(
            this.kQueryData, 
            start
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessOffset(start);
        }
    }
}
