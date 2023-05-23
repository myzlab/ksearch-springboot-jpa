package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import com.myzlab.k.optional.KOptionalKCondition;
import java.util.ArrayList;
import java.util.List;

public class KSetUpdate extends KQueryUpdate {

    private KSetUpdate() {
        super();
    }
    
    private KSetUpdate(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        super(kQueryUpdateData, kExecutor);
        
        this.process(kTableColumn, kColumnAllowedToSetUpdate, null);
    }
    
    private KSetUpdate(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate,
        final String castRule
    ) {
        super(kQueryUpdateData, kExecutor);
        
        this.process(kTableColumn, kColumnAllowedToSetUpdate, castRule);
    }
    
    private KSetUpdate(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KTableColumn kTableColumn,
        final KQuery kQuery
    ) {
        super(kQueryUpdateData, kExecutor);
        
        this.process(kTableColumn, kQuery);
    }
    
    protected static KSetUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        return new KSetUpdate(kExecutor, kQueryUpdateData, kTableColumn, kColumnAllowedToSetUpdate);
    }
    
    protected static KSetUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate,
        final String castRule
    ) {
        return new KSetUpdate(kExecutor, kQueryUpdateData, kTableColumn, kColumnAllowedToSetUpdate, castRule);
    }
    
    protected static KSetUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KTableColumn kTableColumn,
        final KQuery kQuery
    ) {
        return new KSetUpdate(kExecutor, kQueryUpdateData, kTableColumn, kQuery);
    }
    
    public KFromUpdate from(
        final KTable kTable
    ) {
        return KFromUpdate.getInstance(this.k, this.kQueryUpdateData, kTable);
    }
    
    public KFromUpdate from(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return KFromUpdate.getInstance(this.k, this.kQueryUpdateData, new KTable(kRaw.content, new KQueryData(kRaw.params)));
    }
    
    public KFromUpdate from(
        final KCommonTableExpressionFilled kCommonTableExpressionFilled
    ) {
        return KFromUpdate.getInstance(this.k, this.kQueryUpdateData, new KTable(null, kCommonTableExpressionFilled.name, kCommonTableExpressionFilled.alias));
    }
    
    public KReturningUpdate returning(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return KReturningUpdate.getInstance(this.k, this.kQueryUpdateData, kColumnsAllowedToReturning);
    }
    
    public KSetUpdate set(
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate
    ) {
        KUtils.assertNotNull(kColumnAllowedToSetUpdate, "kColumnAllowedToSetUpdate");
        
        this.process(kTableColumn, kColumnAllowedToSetUpdate, null);
        
        return this;
    }
    
    public KSetUpdate set(
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate,
        final String castRule
    ) {
        KUtils.assertNotNull(kColumnAllowedToSetUpdate, "kColumnAllowedToSetUpdate");
        
        this.process(kTableColumn, kColumnAllowedToSetUpdate, castRule);
        
        return this;
    }
    
    public KSetUpdate set(
        final KTableColumn kTableColumn,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        this.process(kTableColumn, kQuery);
        
        return this;
    }
    
    public KSetUpdate set(
        final KTableColumn kTableColumn,
        final Object object
    ) {
        final List<Object> params = new ArrayList();
        
        if (object != null) {
            params.add(object);
        }
        
        final KColumn kColumnValue = new KColumn(new StringBuilder(object == null ? "NULL" : "?"), params, false);
        
        this.process(kTableColumn, kColumnValue, null);
        
        return this;
    }
    
    public KSetUpdate set(
        final KTableColumn kTableColumn,
        final Object object,
        final String castRule
    ) {
        final List<Object> params = new ArrayList();
        
        if (object != null) {
            params.add(object);
        }
        
        final KColumn kColumnValue = new KColumn(new StringBuilder(object == null ? "NULL" : "?"), params, false);
        
        this.process(kTableColumn, kColumnValue, object != null ? castRule : null);
        
        return this;
    }
    
    public KWhereUpdate where(
        final KCondition kCondition
    ) {
        return KWhereUpdate.getInstance(this.k, this.kQueryUpdateData, kCondition);
    }
    
    public KWhereUpdate where(
        final KOptionalKCondition kOptionalKCondition
    ) {
        return KWhereUpdate.getInstance(this.k, this.kQueryUpdateData, !kOptionalKCondition.isPresent() ? KCondition.getEmptyInstance() : kOptionalKCondition.get());
    }
    
    public KWhereUpdate where(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kCondition = new KCondition(kRaw.content, kRaw.params);
        
        return KWhereUpdate.getInstance(this.k, this.kQueryUpdateData, kCondition);
    }
    
    public int execute() {
        return super.executeSingle();
    }
    
    private void process(
        final KTableColumn kTableColumn,
        final KColumnAllowedToSetUpdate kColumnAllowedToSetUpdate,
        final String castRule
    ) {
        KUtils.assertNotNull(kTableColumn, "kTableColumn");
        KUtils.assertNotNull(kColumnAllowedToSetUpdate, "kColumnAllowedToSetUpdate");
        
        if (this.kQueryUpdateData.setValuesAdded == 0) {
            this.kQueryUpdateData.sb.append(" SET ");
        } else {
            this.kQueryUpdateData.sb.append(", ");
        }
        
        this.kQueryUpdateData.setValuesAdded++;
        
        final String v;
        
        if (castRule != null && !castRule.isEmpty()) {
            v = "CAST(" + kColumnAllowedToSetUpdate.getSqlToSet() + " AS " + castRule + ")";
        } else {
            v = kColumnAllowedToSetUpdate.getSqlToSet();
        }
        
        this.kQueryUpdateData.params.addAll(kColumnAllowedToSetUpdate.getParams());
        this.kQueryUpdateData.sb.append(kTableColumn.name).append(" = ").append(v);
    }
    
    private void process(
        final KTableColumn kTableColumn,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kTableColumn, "kTableColumn");
        KUtils.assertNotNull(kQuery, "kQuery");
        
        if (this.kQueryUpdateData.setValuesAdded == 0) {
            this.kQueryUpdateData.sb.append(" SET ");
        } else {
            this.kQueryUpdateData.sb.append(", ");
        }
        
        this.kQueryUpdateData.setValuesAdded++;
        
        final KQueryGenericData subQuery = kQuery.generateSubQueryData();
        
        this.kQueryUpdateData.params.addAll(subQuery.params);
        this.kQueryUpdateData.sb.append(kTableColumn.name).append(" = (").append(subQuery.sb).append(")");
    }
}