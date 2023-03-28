package com.myzlab.k;

import static com.myzlab.k.KFunction.*;
import com.myzlab.k.optional.KOptionalLong;
import java.util.List;

public class KOffset extends KQuery {
    
    private KOffset() {
        super();
    }
    
    private KOffset(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final long start
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
        
        this.process(start);
    }
    
    private KOffset(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
    }
    
    protected static KOffset getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final long start
    ) {
        return new KOffset(kExecutor, kSpecialFunctions, kQueryData, start);
    }
    
    protected static KOffset getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData
    ) {
        return new KOffset(kExecutor, kSpecialFunctions, kQueryData);
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
    
    public KFetch fetch(
        final int rowCount
    ) {
        return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, rowCount);
    }
    
    public KFetch fetch(
        final long rowCount
    ) {
        return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, rowCount);
    }
    
    public KFetch fetch(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData);
        }
        
        return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kOptionalLong.get());
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
