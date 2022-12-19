package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.optional.KOptionalSpecialFunction;
import java.util.ArrayList;
import java.util.List;

public class KSpecialBuilder {
    
    protected final KBuilder k;
    protected final List<KSpecialFunction> kSpecialFunctions;
    
    private KSpecialBuilder(
        final KBuilder k,
        final KSpecialFunction kSpecialFunction
    ) {
        this(k);
        
        this.kSpecialFunctions.add(kSpecialFunction);
    }
    
    private KSpecialBuilder(
        final KBuilder k
    ) {
        this.k = k;
        this.kSpecialFunctions = new ArrayList<>();
    }
    
    protected static KSpecialBuilder getInstance(
        final KBuilder k,
        final KSpecialFunction kSpecialFunction
    ) {
        return new KSpecialBuilder(k, kSpecialFunction);
    }
    
    protected static KSpecialBuilder getInstance(
        final KBuilder k,
        final KOptionalSpecialFunction kOptionalSpecialFunction
    ) {
        if (kOptionalSpecialFunction.isPresent()) {
            return new KSpecialBuilder(k, kOptionalSpecialFunction.get());
        }
        
        return new KSpecialBuilder(k);
    }
    
    public KSpecialBuilder sf(
        final KSpecialFunction kSpecialFunction
    ) {
        this.kSpecialFunctions.add(kSpecialFunction);
        
        return this;
    }
    
    public KSpecialExecutor jdbc(
        final String jdbc
    ) {
        return KSpecialExecutor.getInstance(this, jdbc);
    }
    
    public KSpecialWith with(
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        return KSpecialWith.getInstance(this.k.jdbc(), this.kSpecialFunctions, false, kCommonTableExpressionsFilled);
    }
    
    public KSpecialWith withRecursive(
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        return KSpecialWith.getInstance(this.k.jdbc(), this.kSpecialFunctions, true, kCommonTableExpressionsFilled);
    }
    
    public KSelect select(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getInstance(this.k.jdbc(), this.kSpecialFunctions, kQuery, alias);
    }
    
    public KSelect selectDistinct(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getDistinctInstance(this.k.jdbc(), this.kSpecialFunctions, kQuery, alias);
    }
    
    public KSelect select1() {
        return KSelect.getInstance(this.k.jdbc(), this.kSpecialFunctions, KFunction.val(1));
    }
    
    public KSelect select(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getInstance(this.k.jdbc(), this.kSpecialFunctions, kColumnsAllowedToSelect);
    }
    
    public KSelect selectDistinct(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getDistinctInstance(this.k.jdbc(), this.kSpecialFunctions, kColumnsAllowedToSelect);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KColumn kColumn
    ) {
        return KDistinctOnSelect.getInstance(this.k.jdbc(), this.kSpecialFunctions, kColumn);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KRaw kRaw
    ) {
        return KDistinctOnSelect.getInstance(this.k.jdbc(), this.kSpecialFunctions, kRaw);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final int n
    ) {
        return KDistinctOnSelect.getInstance(this.k.jdbc(), this.kSpecialFunctions, n);
    }
}
