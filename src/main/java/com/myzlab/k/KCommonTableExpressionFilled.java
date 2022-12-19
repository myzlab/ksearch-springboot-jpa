package com.myzlab.k;

public class KCommonTableExpressionFilled extends KCommonTableExpression {

    private KCommonTableExpressionFilled() {
        super();
    }
    
    protected KCommonTableExpressionFilled(
        final KQuery kQuery,
        final String name,
        final String... columns
    ) {
        super(kQuery, name, columns);
    }
    
    protected KCommonTableExpressionFilled(
        final KValues kValues,
        final String name,
        final String... columns
    ) {
        super(kValues, name, columns);
    }
    
    protected static KCommonTableExpressionFilled getInstance(
        final KQuery kQuery,
        final String name,
        final String... columns
    ) {
        return new KCommonTableExpressionFilled(kQuery, name, columns);
    }
        
    protected static KCommonTableExpressionFilled getInstance(
        final KValues kValues,
        final String name,
        final String... columns
    ) {
        return new KCommonTableExpressionFilled(kValues, name, columns);
    }
    
    public KCommonTableExpressionAliased as(
        final String alias
    ) {
        return KCommonTableExpressionAliased.getInstance(this.kValues, this.kQuery, this.name, alias, this.columns);
    }
}
