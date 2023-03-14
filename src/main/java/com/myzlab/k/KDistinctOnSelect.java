package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToSelect;
import java.util.List;

public class KDistinctOnSelect {
    
    private final KExecutor k;
    private final KQueryData kQueryData;
    private final List<KSpecialFunction> kSpecialFunctions;
    
    private KDistinctOnSelect(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions
    ) {
        this.k = kExecutor;
        this.kQueryData = new KQueryData();
        this.kSpecialFunctions = kSpecialFunctions;
    }
    
    private KDistinctOnSelect(
        final KQueryData kQueryData,
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions
    ) {
        this.k = kExecutor;
        this.kQueryData = kQueryData;
        this.kSpecialFunctions = kSpecialFunctions;
    }
    
    protected static KDistinctOnSelect getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KColumn kColumn
    ) {
        final KDistinctOnSelect kDistinctOnSelect = new KDistinctOnSelect(kExecutor, kSpecialFunctions);
        
        kDistinctOnSelect.process(kColumn);
        
        return kDistinctOnSelect;
    }
    
    protected static KDistinctOnSelect getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final int n
    ) {
        final KDistinctOnSelect kDistinctOnSelect = new KDistinctOnSelect(kExecutor, kSpecialFunctions);
        
        kDistinctOnSelect.process(n);
        
        return kDistinctOnSelect;
    }
    
    protected static KDistinctOnSelect getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KDistinctOnSelect kDistinctOnSelect = new KDistinctOnSelect(kExecutor, kSpecialFunctions);
        
        kDistinctOnSelect.process(new KColumn(new StringBuilder(kRaw.content), kRaw.params, false));
        
        return kDistinctOnSelect;
    }
    
    protected static KDistinctOnSelect getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KColumn kColumn
    ) {
        final KDistinctOnSelect kDistinctOnSelect = new KDistinctOnSelect(kQueryData, kExecutor, kSpecialFunctions);
        
        kDistinctOnSelect.process(kColumn);
        
        return kDistinctOnSelect;
    }
    
    protected static KDistinctOnSelect getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final int n
    ) {
        final KDistinctOnSelect kDistinctOnSelect = new KDistinctOnSelect(kQueryData, kExecutor, kSpecialFunctions);
        
        kDistinctOnSelect.process(n);
        
        return kDistinctOnSelect;
    }
    
    protected static KDistinctOnSelect getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KDistinctOnSelect kDistinctOnSelect = new KDistinctOnSelect(kQueryData, kExecutor, kSpecialFunctions);
        
        kDistinctOnSelect.process(new KColumn(new StringBuilder(kRaw.content), kRaw.params, false));
        
        return kDistinctOnSelect;
    }
    
    public KSelect select(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kColumnsAllowedToSelect);
    }
    
    public KSelect select(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQuery, alias);
    }
    
    private void process(
        final KColumn kColumn
    ) {
        KQueryUtils.processSelectDistinctOn(
            this.kQueryData, 
            kColumn
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessSelectDistinctOn(kColumn);
        }
    }
    
    private void process(
        final int n
    ) {
        KQueryUtils.processSelectDistinctOn(
            this.kQueryData, 
            n
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessSelectDistinctOn(n);
        }
    }
}
