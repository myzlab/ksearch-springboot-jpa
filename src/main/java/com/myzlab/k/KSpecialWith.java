package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToSelect;
import java.util.List;

public class KSpecialWith extends KQuery {

    private KSpecialWith(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions
    ) {
        super(kExecutor, kSpecialFunctions);
    }

    protected static KSpecialWith getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final boolean recursive,
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        final KSpecialWith kWith = new KSpecialWith(kExecutor, kSpecialFunctions);

        kWith.process(recursive, kCommonTableExpressionsFilled);

        return kWith;
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
    
    public KSelect selectDistinct(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getDistinctInstance(this.k, this.kSpecialFunctions, this.kQueryData, kColumnsAllowedToSelect);
    }
    
    public KSelect selectDistinct(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getDistinctInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQuery, alias);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KColumn kColumn
    ) {
        return KDistinctOnSelect.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kColumn);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final int n
    ) {
        return KDistinctOnSelect.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, n);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KRaw kRaw
    ) {
        return KDistinctOnSelect.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kRaw);
    }
    
    private void process(
        final boolean recursive,
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        KQueryUtils.processWith(
            this.kQueryData, 
            recursive,
            kCommonTableExpressionsFilled
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessWith(recursive, kCommonTableExpressionsFilled);
        }
    }
}