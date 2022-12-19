package com.myzlab.k;

import com.myzlab.k.optional.KOptionalLong;
import java.util.List;

public class KLimit extends KQuery {
    
    private KLimit() {
        super();
    }
    
    private KLimit(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final long count
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
        
        this.process(count);
    }
    
    private KLimit(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
    }
    
    protected static KLimit getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final long count
    ) {
        return new KLimit(kExecutor, kSpecialFunctions, kQueryData, count);
    }
    
    protected static KLimit getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData
    ) {
        return new KLimit(kExecutor, kSpecialFunctions, kQueryData);
    }
    
    public KOffsetUnfetchable offset(
        final long start
    ) {
        return KOffsetUnfetchable.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, start);
    }
    
    public KOffsetUnfetchable offset(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KOffsetUnfetchable.getInstance(this.k, this.kSpecialFunctions, this.kQueryData);
        }
        
        return KOffsetUnfetchable.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kOptionalLong.get());
    }
    
    private void process(
        final long count
    ) {
        KQueryUtils.processLimit(
            this.kQueryData, 
            count
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessLimit(count);
        }
    }
}
