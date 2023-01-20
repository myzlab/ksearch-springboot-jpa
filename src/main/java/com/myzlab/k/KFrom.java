package com.myzlab.k;

import static com.myzlab.k.KFunction.*;
import com.myzlab.k.allowed.KColumnAllowedToGroupBy;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KQueryAllowedToCombining;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import com.myzlab.k.optional.KOptionalLong;
import java.util.List;

public class KFrom extends KQuery implements KQueryAllowedToCombining {
    
    private KFrom() {
        super();
    }
    
    private KFrom(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KTable kTable
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
        
        this.process(kTable);
    }
    
    protected static KFrom getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KTable kTable
    ) {
        return new KFrom(kExecutor, kSpecialFunctions, kQueryData, kTable);
    }
    
    public KTable as(
        final String alias
    ) {
        return table(this, alias);
    }
    
    public KFrom crossJoin(
        final KTable kTable  
    ) {
        this.processCrossJoin(kTable);
        
        return this;
    }
    
    public KFrom crossJoin(
        final KRaw kRaw
    ) {
        this.processCrossJoin(kRaw);
        
        return this;
    }
    
    public KFrom from(
        final KTable kTable
    ) {
        this.process(kTable);
        
        return this;
    }
    
    public KFrom from(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        this.process(new KTable(null, kRaw.content, null));
        
        return this;
    }
    
    public KFrom from(
        final KCommonTableExpressionFilled kCommonTableExpressionFilled
    ) {
        this.process(new KTable(null, kCommonTableExpressionFilled.name, kCommonTableExpressionFilled.alias));
        
        return this;
    }
    
    public KFrom fullJoin(
        final KJoinDefinition kJoinDefinition
    ) {
        this.processGeneralJoin("FULL JOIN", kJoinDefinition);
        
        return this;
    }
    
    public KFrom fullJoin(
        final KRaw kRaw
    ) {
        this.processGeneralJoin("FULL JOIN", kRaw);
        
        return this;
    }
    
    public KFrom innerJoin(
        final KJoinDefinition kJoinDefinition
    ) {
        this.processGeneralJoin("INNER JOIN", kJoinDefinition);
        
        return this;
    }
    
    public KFrom innerJoin(
        final KRaw kRaw
    ) {
        this.processGeneralJoin("INNER JOIN", kRaw);
        
        return this;
    }
    
    public KFrom leftJoin(
        final KJoinDefinition kJoinDefinition
    ) {
        this.processGeneralJoin("LEFT JOIN", kJoinDefinition);
        
        return this;
    }
    
    public KFrom leftJoin(
        final KRaw kRaw
    ) {
        this.processGeneralJoin("LEFT JOIN", kRaw);
        
        return this;
    }
    
    public KFrom rightJoin(
        final KJoinDefinition kJoinDefinition
    ) {
        this.processGeneralJoin("RIGHT JOIN", kJoinDefinition);
        
        return this;
    }
    
    public KFrom rightJoin(
        final KRaw kRaw
    ) {
        this.processGeneralJoin("RIGHT JOIN", kRaw);
        
        return this;
    }
    
    public KWhere where(
        final KCondition kCondition
    ) {
        return KWhere.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kCondition);
    }
    
    public KWhere where(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kCondition = new KCondition(kRaw.content);
        
        return KWhere.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kCondition);
    }
    
    public KGroupBy groupBy(
        final KColumnAllowedToGroupBy... kColumnsAllowedToGroupBy
    ) {
        return KGroupBy.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kColumnsAllowedToGroupBy);
    }
    
    public KWindow window(
        final KWindowDefinitionAllowedToWindow... KWindowDefinitionsAllowedToWindow
    ) {
        return KWindow.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, KWindowDefinitionsAllowedToWindow);
    }
    
    public KCombining union(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "UNION", false);
    }
    
    public KCombining unionAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "UNION", true);
    }
    
    public KCombining intersect(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "INTERSECT", false);
    }
    
    public KCombining intersectAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "INTERSECT", true);
    }
    
    public KCombining except(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "EXCEPT", false);
    }
    
    public KCombining exceptAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "EXCEPT", true);
    }
    
    public KOrderBy orderBy(
        final KColumnAllowedToOrderBy... kColumnsAllowedToOrderBy
    ) {
        return KOrderBy.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kColumnsAllowedToOrderBy);
    }
    
    public KLimit limit(
        final int count
    ) {
        return KLimit.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, count);
    }
    
    public KLimit limit(
        final long count
    ) {
        return KLimit.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, count);
    }
    
    public KLimit limit(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KLimit.getInstance(this.k, this.kSpecialFunctions, this.kQueryData);
        }
        
        return KLimit.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kOptionalLong.get());
    }
    
    public KOffset offset(
        final int start
    ) {
        return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, start);
    }
    
    public KOffset offset(
        final long start
    ) {
        return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, start);
    }
    
    public KOffset offset(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData);
        }
        
        return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kOptionalLong.get());
    }
    
    public KFetch fetch(
        final int rowCount
    ) {
        return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, rowCount);
    }
    
    public KFetch fetch(
        final long rowCount
    ) {
        return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, rowCount);
    }
    
    public KFetch fetch(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData);
        }
        
        return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kOptionalLong.get());
    }
    
    private void process(
        final KTable kTable
    ) {
        KQueryUtils.processFrom(
            this.kQueryData, 
            kTable
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessFrom(kTable);
        }
    }
    
    private void processGeneralJoin(
        final String joinName,
        final KJoinDefinition kJoinDefinition
    ) {
        KQueryUtils.processGeneralJoinFrom(
            this.kQueryData, 
            joinName,
            kJoinDefinition
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessGeneralJoinFrom(joinName, kJoinDefinition);
        }
    }
    
    private void processGeneralJoin(
        final String joinName,
        final KRaw kRaw
    ) {
        KQueryUtils.processGeneralJoinFrom(
            this.kQueryData, 
            joinName,
            kRaw
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessGeneralJoinFrom(joinName, kRaw);
        }
    }
    
    private void processCrossJoin(
        final KTable kTable
    ) {
        KQueryUtils.processCrossJoinFrom(
            this.kQueryData, 
            kTable
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessCrossJoinFrom(kTable);
        }
    }
    
    private void processCrossJoin(
        final KRaw kRaw
    ) {
        KQueryUtils.processCrossJoinFrom(
            this.kQueryData, 
            kRaw
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessCrossJoinFrom(kRaw);
        }
    }
    
    @Override
    public KQueryData generateSubQueryData() {
        return this.kQueryData.cloneMe();
    }
}
