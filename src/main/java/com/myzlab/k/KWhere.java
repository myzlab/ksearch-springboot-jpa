package com.myzlab.k;

import static com.myzlab.k.KFunction.*;
import com.myzlab.k.allowed.KColumnAllowedToGroupBy;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KQueryAllowedToCombining;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import com.myzlab.k.optional.KOptionalLong;
import java.util.List;

public class KWhere extends KQuery implements KQueryAllowedToCombining {
    
    private final KCondition kCondition;
    
    private KWhere() {
        super();
        
        this.kCondition = null;
    }
    
    private KWhere(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KCondition kCondition
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
        
        KUtils.assertNotNull(kCondition, "kCondition");
        
        this.kCondition = kCondition;
    }
    
    protected static KWhere getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KCondition kCondition
    ) {
        return new KWhere(kExecutor, kSpecialFunctions, kQueryData, kCondition);
    }
    
    public KTable as(
        final String alias
    ) {
        return table(this, alias);
    }
    
    public KWhere andNot(
        final KCondition kCondition
    ) {
        this.kCondition.andNot(kCondition);
        
        return this;
    }
    
    public KWhere andNot(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kRawCondition = new KCondition(kRaw.content);
        
        this.kCondition.andNot(kRawCondition);
        
        return this;
    }
    
    public KWhere orNot(
        final KCondition kCondition
    ) {
        this.kCondition.orNot(kCondition);
        
        return this;
    }
    
    public KWhere orNot(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kRawCondition = new KCondition(kRaw.content);
        
        this.kCondition.orNot(kRawCondition);
        
        return this;
    }

    public KWhere and(
        final KCondition kCondition
    ) {
        this.kCondition.and(kCondition);
        
        return this;
    }
    
    public KWhere and(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kRawCondition = new KCondition(kRaw.content);
        
        this.kCondition.and(kRawCondition);
        
        return this;
    }
    
    public KWhere or(
        final KCondition kCondition
    ) {
        this.kCondition.or(kCondition);
        
        return this;
    }
    
    public KWhere or(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kRawCondition = new KCondition(kRaw.content);
        
        this.kCondition.or(kRawCondition);
        
        return this;
    }
    
    public KGroupBy groupBy(
        final KColumnAllowedToGroupBy... KColumnsAllowedToGroupBy
    ) {
        this.buildWhere();
        
        return KGroupBy.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, KColumnsAllowedToGroupBy);
    }
    
    public KWindow window(
        final KWindowDefinitionAllowedToWindow... KWindowDefinitionsAllowedToWindow
    ) {
        this.buildWhere();
        
        return KWindow.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, KWindowDefinitionsAllowedToWindow);
    }
    
    public KCombining union(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWhere();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "UNION", false);
    }
    
    public KCombining unionAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWhere();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "UNION", true);
    }
    
    public KCombining intersect(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWhere();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "INTERSECT", false);
    }
    
    public KCombining intersectAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWhere();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "INTERSECT", true);
    }
    
    public KCombining except(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWhere();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "EXCEPT", false);
    }
    
    public KCombining exceptAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        this.buildWhere();
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQueryAllowedToCombining, "EXCEPT", true);
    }
    
    public KOrderBy orderBy(
        final KColumnAllowedToOrderBy... kColumnsAllowedToOrderBy
    ) {
        this.buildWhere();
        
        return KOrderBy.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kColumnsAllowedToOrderBy);
    }
    
    public KLimit limit(
        final int count
    ) {
        this.buildWhere();
        
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
        this.buildWhere();
        
        if (!kOptionalLong.isPresent()) {
            return KLimit.getInstance(this.k, this.kSpecialFunctions, this.kQueryData);
        }
        
        return KLimit.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kOptionalLong.get());
    }
    
    public KOffset offset(
        final int start
    ) {
        this.buildWhere();
        
        return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, start);
    }
    
    public KOffset offset(
        final long start
    ) {
        this.buildWhere();
        
        return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, start);
    }
    
    public KOffset offset(
        final KOptionalLong kOptionalLong
    ) {
        this.buildWhere();
        
        if (!kOptionalLong.isPresent()) {
            return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData);
        }
        
        return KOffset.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kOptionalLong.get());
    }
    
    public KFetch fetch(
        final int rowCount
    ) {
        this.buildWhere();
        
        return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, rowCount);
    }
    
    public KFetch fetch(
        final long rowCount
    ) {
        this.buildWhere();
        
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
    
    private void buildWhere() {
        KQueryUtils.buildWhere(
            this.kQueryData, 
            this.kCondition
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onBuildWhere(this.kCondition);
        }
    }
    
    @Override
    public <T> T single(
        final Class<T> clazz
    ) {
        this.buildWhere();
        
        return super.single(clazz);
    }
    
    @Override
    public <T extends KRow> KCollection<T> multiple(
        final Class<T> clazz
    ) {
        this.buildWhere();
        
        return super.multiple(clazz);
    }
    
    @Override
    public KQueryData generateSubQueryData() {
        final KQueryData newKQueryData = this.kQueryData.cloneMe();
        
        KQueryUtils.buildWhere(
            newKQueryData, 
            this.kCondition
        );
        
        return newKQueryData;
    }
}
