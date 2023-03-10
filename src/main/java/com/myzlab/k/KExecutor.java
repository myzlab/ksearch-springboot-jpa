package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToSelect;
import java.util.ArrayList;
import javax.persistence.EntityManager;

public class KExecutor {
    
    private final KBuilder k;
    private final String jdbc;

    private KExecutor(
        final KBuilder k,
        final String jdbc
    ) {
        this.k =  k;
        this.jdbc = jdbc;
    }
    
    protected EntityManager getEntityManager() {
        return k.getEntityManagers().get(jdbc);
    }
    
    protected static KExecutor getInstance(
        final KBuilder kBuilder,
        final String jdbc
    ) {
        return new KExecutor(kBuilder, jdbc);
    }
    
    public KWith with(
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        return KWith.getInstance(this, false, kCommonTableExpressionsFilled);
    }
    
    public KWith withRecursive(
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        return KWith.getInstance(this, true, kCommonTableExpressionsFilled);
    }
    
    public KSelect select(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getInstance(this, new ArrayList<>(), kQuery, alias);
    }
    
    public KSelect selectDistinct(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getDistinctInstance(this, new ArrayList<>(), kQuery, alias);
    }
    
    public KSelect select1() {
        return KSelect.getInstance(this, new ArrayList<>(), KFunction.val(1));
    }
    
    public KSelect select(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getInstance(this, new ArrayList<>(), kColumnsAllowedToSelect);
    }
    
    public KSelect selectDistinct(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getDistinctInstance(this, new ArrayList<>(), kColumnsAllowedToSelect);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KColumn kColumn
    ) {
        return KDistinctOnSelect.getInstance(this, new ArrayList<>(), kColumn);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KRaw kRaw
    ) {
        return KDistinctOnSelect.getInstance(this, new ArrayList<>(), kRaw);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final int n
    ) {
        return KDistinctOnSelect.getInstance(this, new ArrayList<>(), n);
    }
    
    public KDeleteFrom deleteFrom(
        final KTable kTable
    ) {
        return KDeleteFrom.getInstance(this, kTable);
    }
    
    public KDeleteFrom deleteFrom(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return KDeleteFrom.getInstance(this, new KTable(null, kRaw.content, null));
    }
    
    public KInsertInto insertInto(
        final KTable kTable
    ) {
        return KInsertInto.getInstance(this, kTable);
    }
    
    public KUpdate update(
        final KTable kTable
    ) {
        return KUpdate.getInstance(this, kTable);
    }
    
    public KUpdate update(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return KUpdate.getInstance(this, new KTable(null, kRaw.content, null));
    }
    
}
