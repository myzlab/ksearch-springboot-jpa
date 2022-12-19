package com.myzlab.k;

public class KCommonTableExpressionNamed extends KCommonTableExpression {

    private KCommonTableExpressionNamed(
    ) {
        super();
    }
    
    protected KCommonTableExpressionNamed(
        final String name
    ) {
        super(name);
    }
        
    protected static KCommonTableExpressionNamed getInstance(
        final String name
    ) {
        return new KCommonTableExpressionNamed(name);
    }
    
    public KCommonTableExpressionColumned columns(
        final String... columns
    ) {
        return KCommonTableExpressionColumned.getInstance(this.name, columns);
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
