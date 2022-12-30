package com.myzlab.k;

public class KCommonTableExpressionFilled extends KCommonTableExpression {

    private KCommonTableExpressionFilled() {
        super();
    }
    
    protected KCommonTableExpressionFilled(
        final KQuery kQuery,
        final String name,
        final String alias,
        final String... columns
    ) {
        super(null, kQuery, name, alias, columns);
    }
    
    protected KCommonTableExpressionFilled(
        final KValues kValues,
        final String name,
        final String alias,
        final String... columns
    ) {
        super(kValues, null, name, alias, columns);
    }
    
    protected static KCommonTableExpressionFilled getInstance(
        final KQuery kQuery,
        final String name,
        final String alias,
        final String... columns
    ) {
        return new KCommonTableExpressionFilled(kQuery, name, alias, columns);
    }
        
    protected static KCommonTableExpressionFilled getInstance(
        final KValues kValues,
        final String name,
        final String alias,
        final String... columns
    ) {
        return new KCommonTableExpressionFilled(kValues, name, alias, columns);
    }
    
    public KColumn c(
        final String name
    ) {
        return this.processColumn(name);
    }
    
    public KColumn column(
        final String name
    ) {
        return this.processColumn(name);
    }
    
    public KJoinDefinition on(
        final KCondition kCondition
    ) {
        return KJoinDefinition.getInstance(new KTable(null, this.name, this.alias).toSql(true), kCondition);
    }
    
    private KColumn processColumn(
        final String name
    ) {
        final StringBuilder sb = new StringBuilder();
        
        sb.append(this.alias).append(".").append(name);
        
        return new KColumn(sb, true);
    }
    
}
