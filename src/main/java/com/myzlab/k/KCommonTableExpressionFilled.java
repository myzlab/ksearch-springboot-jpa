package com.myzlab.k;

public class KCommonTableExpressionFilled extends KCommonTableExpression {

    private KCommonTableExpressionFilled() {
        super();
    }
    
    protected KCommonTableExpressionFilled(
        final KGenericQuery kGenericQuery,
        final String name,
        final String alias,
        final String... columns
    ) {
        super(null, kGenericQuery, name, alias, columns);
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
        final KGenericQuery kGenericQuery,
        final String name,
        final String alias,
        final String... columns
    ) {
        return new KCommonTableExpressionFilled(kGenericQuery, name, alias, columns);
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
    
    public KJoinDefinition on(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return KJoinDefinition.getInstance(new KTable(null, this.name, this.alias).toSql(true), new KCondition(kRaw.content));
    }
    
    private KColumn processColumn(
        final String name
    ) {
        final StringBuilder sb = new StringBuilder();
        
        sb.append(this.alias).append(".").append(name);
        
        return new KColumn(sb, true);
    }
    
}
