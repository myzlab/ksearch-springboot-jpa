package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToSelect;

public class KSpecialExecutor {
    
    private final KSpecialBuilder k;
    private final String jdbc;

    private KSpecialExecutor(
        final KSpecialBuilder k,
        final String jdbc
    ) {
        this.k = k;
        this.jdbc = jdbc;
    }
    
    protected static KSpecialExecutor getInstance(
        final KSpecialBuilder kSpecialBuilder,
        final String jdbc
    ) {
        return new KSpecialExecutor(kSpecialBuilder, jdbc);
    }
    
    protected KExecutor jdbc() {
        return KExecutor.getInstance(this.k.k, jdbc);
    }
    
    public KSpecialWith with(
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        return KSpecialWith.getInstance(jdbc(), this.k.kSpecialFunctions, false, kCommonTableExpressionsFilled);
    }
    
    public KSpecialWith withRecursive(
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        return KSpecialWith.getInstance(jdbc(), this.k.kSpecialFunctions, true, kCommonTableExpressionsFilled);
    }
    
    public KSelect select(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getInstance(jdbc(), this.k.kSpecialFunctions, kQuery, alias);
    }
    
    public KSelect selectDistinct(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getDistinctInstance(jdbc(), this.k.kSpecialFunctions, kQuery, alias);
    }
    
    public KSelect select(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getInstance(jdbc(), this.k.kSpecialFunctions, kColumnsAllowedToSelect);
    }
    
    public KSelect selectDistinct(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getDistinctInstance(jdbc(), this.k.kSpecialFunctions, kColumnsAllowedToSelect);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KColumn kColumn
    ) {
        return KDistinctOnSelect.getInstance(jdbc(), this.k.kSpecialFunctions, kColumn);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KRaw kRaw
    ) {
        return KDistinctOnSelect.getInstance(jdbc(), this.k.kSpecialFunctions, kRaw);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final int n
    ) {
        return KDistinctOnSelect.getInstance(jdbc(), this.k.kSpecialFunctions, n);
    }
    
}
