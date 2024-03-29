package com.myzlab.k;

import static com.myzlab.k.KFunction.*;
import com.myzlab.k.allowed.KColumnAllowedToGroupBy;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KQueryAllowedToCombining;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import com.myzlab.k.optional.KOptionalKCondition;
import com.myzlab.k.optional.KOptionalLong;
import java.util.List;

public class KGroupBy extends KQuery implements KQueryAllowedToCombining {
    
    private KGroupBy(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KColumnAllowedToGroupBy... kColumnsAllowedToGroupBy
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
        
        KUtils.assertNotNull(kColumnsAllowedToGroupBy, "kColumnsAllowedToGroupBy");
        
        this.process(kColumnsAllowedToGroupBy);
    }
    
    protected static KGroupBy getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KColumnAllowedToGroupBy... kColumnsAllowedToGroupBy
    ) {
        return new KGroupBy(kExecutor, kSpecialFunctions, kQueryData, kColumnsAllowedToGroupBy);
    }
    
    public KTable as(
        final String alias
    ) {
        return table(this, alias);
    }
    
    public KTable as(
        final String alias,
        final String... names
    ) {
        return table(this, alias, tuple(names));
    }

    public KHaving having(
        final KCondition kCondition
    ) {
        return KHaving.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kCondition);
    }
    
    public KHaving having(
        final KOptionalKCondition kOptionalKCondition
    ) {
        return KHaving.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, !kOptionalKCondition.isPresent() ? KCondition.getEmptyInstance() : kOptionalKCondition.get());
    }
    
    public KHaving having(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kCondition = new KCondition(kRaw.content, kRaw.params);
        
        return KHaving.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kCondition);
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
        final KColumnAllowedToGroupBy... kColumnsAllowedToGroupBy
    ) {
        KQueryUtils.processGroupBy(
            this.kQueryData, 
            kColumnsAllowedToGroupBy
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessGroupBy(kColumnsAllowedToGroupBy);
        }
    }
    
    @Override
    public KQueryData generateSubQueryData() {
        return this.kQueryData.cloneMe();
    }
}
