package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToGroupBy;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KQueryAllowedToCombining;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import java.util.List;
import javax.persistence.Query;

public class KTotalCount extends KSpecialFunction {

    @Override
    protected void onProcessWith(
        final boolean recursive,
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        KQueryUtils.processWith(
            this.kQueryData, 
            recursive,
            kCommonTableExpressionsFilled
        );
    }
    
    @Override
    protected void onProcessSelectDistinctOn(
        final KColumn kColumn
    ) {
        KQueryUtils.processSelectDistinctOn(
            this.kQueryData, 
            kColumn
        );
    }
    
    @Override
    protected void onProcessSelectDistinctOn(
        final int n
    ) {
        KQueryUtils.processSelectDistinctOn(
            this.kQueryData, 
            n
        );
    }

    @Override
    protected void onProcessSelect(
        final boolean distinct,
        final List<KBaseColumn> kBaseColumns
    ) {
        KQueryUtils.processSelect(
            this.kQueryData, 
            distinct,
            kBaseColumns
        );
    }

    @Override
    protected void onProcessSelect(
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
    }

    @Override
    protected void onProcessFrom(
        final KTable kTable
    ) {
        KQueryUtils.processFrom(
            this.kQueryData, 
            kTable
        );
    }
    
    @Override
    protected void onProcessGeneralJoinFrom(
        final String joinName,
        final KJoinDefinition kJoinDefinition
    ) {
        KQueryUtils.processGeneralJoinFrom(
            this.kQueryData, 
            joinName,
            kJoinDefinition
        );
    }
    
    @Override
    protected void onProcessGeneralJoinFrom(
        final String joinName,
        final KRaw kRaw
    ) {
        KQueryUtils.processGeneralJoinFrom(
            this.kQueryData, 
            joinName,
            kRaw
        );
    }
    
    @Override
    protected void onProcessCrossJoinFrom(
        final KTable kTable
    ) {
        KQueryUtils.processCrossJoinFrom(
            this.kQueryData, 
            kTable
        );
    }
    
    @Override
    protected void onProcessCrossJoinFrom(
        final KRaw kRaw
    ) {
        KQueryUtils.processCrossJoinFrom(
            this.kQueryData, 
            kRaw
        );
    }
    
    @Override
    protected void onBuildWhere(
        final KCondition kCondition
    ) {
        KQueryUtils.buildWhere(
            this.kQueryData, 
            kCondition
        );
    }
    
    @Override
    protected void onProcessGroupBy(
        final KColumnAllowedToGroupBy... kColumnsAllowedToGroupBy
    ) {
        KQueryUtils.processGroupBy(
            this.kQueryData, 
            kColumnsAllowedToGroupBy
        );
    }
    
    @Override
    protected void onBuildHaving(
        final KCondition kCondition
    ) {
        KQueryUtils.buildHaving(
            this.kQueryData, 
            kCondition
        );
    }
    
    @Override
    protected void onBuildWindow(
        final List<KWindowDefinitionAllowedToWindow> kWindowDefinitionsAllowedToWindow
    ) {
        KQueryUtils.buildWindow(
            this.kQueryData, 
            kWindowDefinitionsAllowedToWindow
        );
    }
    
    @Override
    protected void onProcessCombining(
        final KQueryAllowedToCombining kQueryAllowedToCombining,
        final String function,
        final boolean all
    ) {
        KQueryUtils.processCombining(
            this.kQueryData, 
            kQueryAllowedToCombining,
            function,
            all
        );
    }
    
    @Override
    protected void onProcessOrderBy(
        final KColumnAllowedToOrderBy... kColumnsAllowedToOrderBy
    ) {
        //NOTHING TODO
    }
    
    @Override
    protected void onProcessLimit(
        final long count
    ) {
        //NOTHING TODO
    }
    
    @Override
    protected void onProcessOffset(
        final long start
    ) {
        //NOTHING TODO
    }
    
    @Override
    protected void onProcessFetch(
        final long rowCount
    ) {
        //NOTHING TODO
    }
    
    @Override
    protected void executeOnMultipleMapping(
        final KExecutor k,
        final KCollection kCollection
    ) {
        final StringBuilder sb = new StringBuilder(this.kQueryData.sb);
        
        sb.insert(0, "SELECT COUNT(*) FROM (").append(") AS GOD_BLESS_YOU");
        
//        System.out.println(sb.toString());
//        System.out.println(this.kQueryData.params);
//        System.out.println(kQueryGenericData.kEdges);
//        System.out.println(kQueryGenericData.kNodes);
        
        final Query query = k.getEntityManager().createNativeQuery(this.kQueryData.sb.toString());
        
        int i = 1;
        
        for (final Object param : this.kQueryData.params) {
            query.setParameter(i++, param);
        }
        
        kCollection.addMetadata("totalCount", (Long) query.getSingleResult());
    }
}