package com.myzlab.k;

public class KCommonTableExpressionColumned extends KCommonTableExpression {
    
    private KCommonTableExpressionColumned() {
        super();
    }
    
    protected KCommonTableExpressionColumned(
        final String name,
        final String... columns
    ) {
        super(name, columns);
    }
        
    protected static KCommonTableExpressionColumned getInstance(
        final String name,
        final String... columns
    ) {
        return new KCommonTableExpressionColumned(name, columns);
    }
    
    public KCommonTableExpressionFilled as(
        final KValues kValues
    ) {
        return KCommonTableExpressionFilled.getInstance(kValues, this.name, this.columns);
    }
    
    public KCommonTableExpressionFilled as(
        final KQuery kQuery
    ) {
        return KCommonTableExpressionFilled.getInstance(kQuery, this.name, this.columns);
    }
    
}
