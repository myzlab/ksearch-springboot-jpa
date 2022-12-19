package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToGroupBy;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KQueryAllowedToCombining;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import java.util.List;

public abstract class KSpecialFunction {
    
    protected KQueryData kQueryData;
    
    public KSpecialFunction() {
        super();
        
        this.kQueryData = new KQueryData();
    }
    
    protected abstract void onProcessWith(
        final boolean recursive,
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    );
    
    protected abstract void onProcessSelectDistinctOn(
        final KColumn kColumn
    );
    
    protected abstract void onProcessSelectDistinctOn(
        final int n
    );
    
    protected abstract void onProcessSelect(
        final boolean distinct,
        final List<KBaseColumn> kBaseColumns
    );
    
    protected abstract void onProcessSelect(
        final boolean distinct,
        final KQuery kQuery,
        final String alias
    );
    
    protected abstract void onProcessFrom(
        final KTable kTable
    );
    
    protected abstract void onProcessGeneralJoinFrom(
        final String joinName,
        final KJoinDefinition kJoinDefinition
    );
    
    protected abstract void onProcessGeneralJoinFrom(
        final String joinName,
        final KRaw kRaw
    );
    
    protected abstract void onProcessCrossJoinFrom(
        final KTable kTable
    );
    
    protected abstract void onProcessCrossJoinFrom(
        final KRaw kRaw
    );
    
    protected abstract void onBuildWhere(
        final KCondition kCondition
    );
    
    protected abstract void onProcessGroupBy(
        final KColumnAllowedToGroupBy... kColumnsAllowedToGroupBy
    );
    
    protected abstract void onBuildHaving(
        final KCondition kCondition
    );
    
    protected abstract void onBuildWindow(
        final List<KWindowDefinitionAllowedToWindow> kWindowDefinitionsAllowedToWindow
    );
    
    protected abstract void onProcessCombining(
        final KQueryAllowedToCombining kQueryAllowedToCombining,
        final String function,
        final boolean all
    );
    
    protected abstract void onProcessOrderBy(
        final KColumnAllowedToOrderBy... kColumnsAllowedToOrderBy
    );
    
    protected abstract void onProcessLimit(
        final long count
    );
    
    protected abstract void onProcessOffset(
        final long start
    );
    
    protected abstract void onProcessFetch(
        final long rowCount
    );
    
    protected abstract void executeOnMultipleMapping(
        final KExecutor k,
        final KCollection kCollection
    );
}
