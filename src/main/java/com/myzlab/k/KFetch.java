package com.myzlab.k;

import java.util.List;

public class KFetch extends KQuery {

    private KFetch() {
        super();
    }
    
    private KFetch(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final long rowCount
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
        
        this.process(rowCount);
    }
    
    private KFetch(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
    }
    
    public static KFetch getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final long rowCount
    ) {
        return new KFetch(kExecutor, kSpecialFunctions, kQueryData, rowCount);
    }
    
    public static KFetch getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData
    ) {
        return new KFetch(kExecutor, kSpecialFunctions, kQueryData);
    }
    
    private void process(
        final long rowCount
    ) {
        KQueryUtils.processFetch(
            this.kQueryData, 
            rowCount
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessFetch(rowCount);
        }
    }
}
