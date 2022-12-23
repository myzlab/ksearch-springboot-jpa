package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.optional.KOptionalSpecialFunction;
import java.util.ArrayList;
import java.util.Map;
import javax.persistence.EntityManager;

public abstract class KBuilder {
    
    public KExecutor jdbc(
        final String jdbc
    ) {
        return KExecutor.getInstance(this, jdbc);
    }
    
    protected KExecutor jdbc() {
        return KExecutor.getInstance(this, this.getEntityManagerDefaultName());
    }
    
    public KSpecialBuilder sf(
        final KSpecialFunction kSpecialFunction
    ) {
        return KSpecialBuilder.getInstance(this, kSpecialFunction);
    }
    
    public KSpecialBuilder sf(
        final KOptionalSpecialFunction kOptionalSpecialFunction
    ) {
        return KSpecialBuilder.getInstance(this, kOptionalSpecialFunction);
    }
    
    public KWith with(
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        return KWith.getInstance(jdbc(), false, kCommonTableExpressionsFilled);
    }
    
    public KWith withRecursive(
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        return KWith.getInstance(jdbc(), true, kCommonTableExpressionsFilled);
    }
    
    public KSelect select(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getInstance(jdbc(), new ArrayList<>(), kQuery, alias);
    }
    
    public KSelect selectDistinct(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getDistinctInstance(jdbc(), new ArrayList<>(), kQuery, alias);
    }
    
    public KSelect select1() {
        return KSelect.getInstance(jdbc(), new ArrayList<>(), KFunction.val(1));
    }
    
    public KSelect select(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getInstance(jdbc(), new ArrayList<>(), kColumnsAllowedToSelect);
    }
    
    public KSelect selectDistinct(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getDistinctInstance(jdbc(), new ArrayList<>(), kColumnsAllowedToSelect);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KColumn kColumn
    ) {
        return KDistinctOnSelect.getInstance(jdbc(), new ArrayList<>(), kColumn);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KRaw kRaw
    ) {
        return KDistinctOnSelect.getInstance(jdbc(), new ArrayList<>(), kRaw);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final int n
    ) {
        return KDistinctOnSelect.getInstance(jdbc(), new ArrayList<>(), n);
    }
    
    public KDeleteFrom deleteFrom(
        final KTable kTable
    ) {
        return KDeleteFrom.getInstance(jdbc(), kTable);
    }
    
    public KDeleteFrom deleteFrom(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return KDeleteFrom.getInstance(jdbc(), new KTable(null, kRaw.content, null));
    }
    
    public KInsertInto insertInto(
        final KTable kTable
    ) {
        return KInsertInto.getInstance(jdbc(), kTable);
    }
    
    public KUpdate update(
        final KTable kTable
    ) {
        return KUpdate.getInstance(jdbc(), kTable);
    }
    
    public KUpdate update(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return KUpdate.getInstance(jdbc(), new KTable(null, kRaw.content, null));
    }
    
    public abstract Map<String, EntityManager> getEntityManagers();
    
    public abstract String getEntityManagerDefaultName();
}
