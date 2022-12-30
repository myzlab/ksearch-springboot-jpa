package com.myzlab.k;

import static com.myzlab.k.KFunction.*;
import com.myzlab.k.allowed.KColumnAllowedToGroupBy;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KQueryAllowedToCombining;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import com.myzlab.k.optional.KOptionalLong;
import java.util.ArrayList;
import java.util.List;

public class KSelect extends KQuery implements KQueryAllowedToCombining {
    
    private KSelect(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions
    ) {
        super(kExecutor, kSpecialFunctions);
    }
    
    private KSelect(
        final KQueryData kQueryData,
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
    }
    
    protected static KSelect getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        KUtils.assertNotNullNotEmpty(kColumnsAllowedToSelect, "kColumnsAllowedToSelect", false);
         
        final List<KBaseColumn> kBaseColumns = new ArrayList<>();
        
        for (final KColumnAllowedToSelect kColumnAllowedToSelect : kColumnsAllowedToSelect) {
            kBaseColumns.add(kColumnAllowedToSelect.getKBaseColumn());
        }
         
        final KSelect kSelect = new KSelect(kExecutor, kSpecialFunctions);
        
        kSelect.process(false, kBaseColumns);
        
        return kSelect;
    }
    
    protected static KSelect getDistinctInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        KUtils.assertNotNullNotEmpty(kColumnsAllowedToSelect, "kColumnsAllowedToSelect", false);
         
        final List<KBaseColumn> kBaseColumns = new ArrayList<>();
        
        for (final KColumnAllowedToSelect kColumnAllowedToSelect : kColumnsAllowedToSelect) {
            kBaseColumns.add(kColumnAllowedToSelect.getKBaseColumn());
        }
         
        final KSelect kSelect = new KSelect(kExecutor, kSpecialFunctions);
        
        kSelect.process(true, kBaseColumns);
        
        return kSelect;
    }
    
    protected static KSelect getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        KUtils.assertNotNullNotEmpty(kColumnsAllowedToSelect, "kColumnsAllowedToSelect", false);
         
        final List<KBaseColumn> kBaseColumns = new ArrayList<>();
        
        for (final KColumnAllowedToSelect kColumnAllowedToSelect : kColumnsAllowedToSelect) {
            kBaseColumns.add(kColumnAllowedToSelect.getKBaseColumn());
        }
         
        final KSelect kSelect = new KSelect(kQueryData, kExecutor, kSpecialFunctions);
        
        kSelect.process(false, kBaseColumns);
        
        return kSelect;
    }
    
    protected static KSelect getDistinctInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        KUtils.assertNotNullNotEmpty(kColumnsAllowedToSelect, "kColumnsAllowedToSelect", false);
        
        final List<KBaseColumn> kBaseColumns = new ArrayList<>();
        
        for (final KColumnAllowedToSelect kColumnAllowedToSelect : kColumnsAllowedToSelect) {
            kBaseColumns.add(kColumnAllowedToSelect.getKBaseColumn());
        }
        
        final KSelect kSelect = new KSelect(kQueryData, kExecutor, kSpecialFunctions);
        
        kSelect.process(true, kBaseColumns);
        
        return kSelect;
    }
    
    protected static KSelect getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQuery kQuery,
        final String alias
    ) {
        final KSelect kSelect = new KSelect(kExecutor, kSpecialFunctions);
        
        kSelect.process(false, kQuery, alias);
        
        return kSelect;
    }
    
    protected static KSelect getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KQuery kQuery,
        final String alias
    ) {
        final KSelect kSelect = new KSelect(kQueryData, kExecutor, kSpecialFunctions);
        
        kSelect.process(false, kQuery, alias);
        
        return kSelect;
    }
    
    protected static KSelect getDistinctInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQuery kQuery,
        final String alias
    ) {
        final KSelect kSelect = new KSelect(kExecutor, kSpecialFunctions);
        
        kSelect.process(true, kQuery, alias);
        
        return kSelect;
    }
    
    protected static KSelect getDistinctInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KQuery kQuery,
        final String alias
    ) {
        final KSelect kSelect = new KSelect(kQueryData, kExecutor, kSpecialFunctions);
        
        kSelect.process(true, kQuery, alias);
        
        return kSelect;
    }
    
    public KTable as(
        final String alias
    ) {
        return table(this, alias);
    }
    
    public KSelect select(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        KUtils.assertNotNullNotEmpty(kColumnsAllowedToSelect, "kColumnsAllowedToSelect", false);
        
        final List<KBaseColumn> kBaseColumns = new ArrayList<>();
        
        for (final KColumnAllowedToSelect kColumnAllowedToSelect : kColumnsAllowedToSelect) {
            kBaseColumns.add(kColumnAllowedToSelect.getKBaseColumn());
        }
        
        this.process(false, kBaseColumns);
        
        return this;
    }
    
    public KSelect select(
        final KQuery kQuery,
        final String alias
    ) {
        this.process(false, kQuery, alias);
        
        return this;
    }

    public KFrom from(
        final KTable kTable
    ) {
        return KFrom.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kTable);
    }
    
    public KFrom from(
        final KRaw kRaw
    ) {
        KUtils.assertNotNullNotEmpty(kRaw, "kRaw", false);
        
        return KFrom.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, new KTable(null, kRaw.content, null));
    }
    
    public KFrom from(
        final KCommonTableExpressionFilled kCommonTableExpressionFilled
    ) {
        return KFrom.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, new KTable(null, kCommonTableExpressionFilled.name, kCommonTableExpressionFilled.alias));
    }
    
    public KGroupBy groupBy(
        final KColumnAllowedToGroupBy... KColumnsAllowedToGroupBy
    ) {
        return KGroupBy.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, KColumnsAllowedToGroupBy);
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
        return KOrderBy.getInstance(this.k, this.kSpecialFunctions, kQueryData, kColumnsAllowedToOrderBy);
    }
    
    public KLimit limit(
        final int count
    ) {
        return KLimit.getInstance(this.k, this.kSpecialFunctions, kQueryData, count);
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
        return KOffset.getInstance(this.k, this.kSpecialFunctions, kQueryData, start);
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
        return KFetch.getInstance(this.k, this.kSpecialFunctions, kQueryData, rowCount);
    }
    
    public KFetch fetch(
        final KOptionalLong kOptionalLong
    ) {
        if (!kOptionalLong.isPresent()) {
            return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData);
        }
        
        return KFetch.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kOptionalLong.get());
    }
    
    public KWhere where(
        final KCondition kCondition
    ) {
        return KWhere.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kCondition);
    }
    
    public KWindow window(
        final KWindowDefinitionAllowedToWindow... KWindowDefinitionsAllowedToWindow
    ) {
        return KWindow.getInstance(this.k, this.kSpecialFunctions, kQueryData, KWindowDefinitionsAllowedToWindow);
    }
    
    private void process(
        final boolean distinct,
        final List<KBaseColumn> kBaseColumns
    ) {
        KQueryUtils.processSelect(
            this.kQueryData, 
            distinct,
            kBaseColumns
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessSelect(distinct, kBaseColumns);
        }
    }
    
    private void process(
        final boolean distinct,
        final KQuery kQuery,
        final String alias
    ) {
        KQueryUtils.processSelect(
            this.kQueryData, 
            distinct,
            kQuery,
            alias
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onProcessSelect(distinct, kQuery, alias);
        }
    }

    @Override
    public KQueryData generateSubQueryData() {
        return this.kQueryData.cloneMe();
    }
    
}
