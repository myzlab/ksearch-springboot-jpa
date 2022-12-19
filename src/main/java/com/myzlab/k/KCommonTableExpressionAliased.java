package com.myzlab.k;

public class KCommonTableExpressionAliased extends KCommonTableExpression {

    private KCommonTableExpressionAliased() {
        super();
    }
    
    protected KCommonTableExpressionAliased(
        final KValues kValues,
        final KQuery kQuery,
        final String name,
        final String alias,
        final String... columns
    ) {
        super(kValues, kQuery, name, alias, columns);
    }
        
    protected static KCommonTableExpressionAliased getInstance(
        final KValues kValues,
        final KQuery kQuery,
        final String name,
        final String alias,
        final String... columns
    ) {
        return new KCommonTableExpressionAliased(kValues, kQuery, name, alias, columns);
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
