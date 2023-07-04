package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KInsertIntoFilled extends KQueryInsertInto {

    private KInsertIntoFilled(
        final KExecutor kExecutor
    ) {
        super(kExecutor);
    }
 
    private KInsertIntoFilled(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KQuery kQuery
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kQuery);
    }
    
    private KInsertIntoFilled(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KValues kValues
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kValues, new HashMap<>());
    }
    
    private KInsertIntoFilled(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KValues kValues,
        final Map<Integer, String> columnsDataType
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kValues, columnsDataType);
    }
    
    protected static KInsertIntoFilled getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KQuery kQuery
    ) {
        return new KInsertIntoFilled(kExecutor, kQueryInsertIntoData, kQuery);
    }
    
    protected static KInsertIntoFilled getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KValues kValues
    ) {
        return new KInsertIntoFilled(kExecutor, kQueryInsertIntoData, kValues);
    }
    
    protected static KInsertIntoFilled getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KValues kValues,
        final Map<Integer, String> columnsDataType
    ) {
        return new KInsertIntoFilled(kExecutor, kQueryInsertIntoData, kValues, columnsDataType);
    }
    
    public KInsertIntoOnConflict onConflict() {
        return KInsertIntoOnConflict.getInstance(this.k, this.kQueryInsertIntoData);
    }
    
    public KReturningInsertInto returning(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return KReturningInsertInto.getInstance(this.k, this.kQueryInsertIntoData, kColumnsAllowedToReturning);
    }
    
    private void process(
        final KValues kValues,
        final Map<Integer, String> columnsDataType
    ) {
        this.kQueryInsertIntoData.sb.append(" VALUES ");
        
        final List<List<Object>> allValues = kValues.values;
            
        for (int i = 0; i < allValues.size(); i++) {
            final List<Object> values = allValues.get(i);
            
            if (i > 0) {
                this.kQueryInsertIntoData.sb.append(", ");
            }

            this.kQueryInsertIntoData.sb.append("(");

            for (int j = 0; j < values.size(); j++) {
                if (j > 0) {
                    this.kQueryInsertIntoData.sb.append(", ");
                }
                
                final Object value = values.get(j);
                
                if (value == null) {
                    this.kQueryInsertIntoData.sb.append("NULL");
                } else if (value instanceof KRaw) {
                    this.kQueryInsertIntoData.sb.append(((KRaw) value).content);
                    this.kQueryInsertIntoData.params.addAll(((KRaw) value).params);
                } else if (value instanceof KColumn) {
                    this.kQueryInsertIntoData.sb.append(((KColumn) value).sb);
                    this.kQueryInsertIntoData.params.addAll(((KColumn) value).params);
                } else {
                    if (columnsDataType.containsKey(j)) {
                        this.kQueryInsertIntoData.sb.append("CAST(? AS ").append(columnsDataType.get(j)).append(")");
                    } else {
                        this.kQueryInsertIntoData.sb.append("?");
                    }
                    
                    this.kQueryInsertIntoData.params.add(value);
                }
            }

            this.kQueryInsertIntoData.sb.append(")");
        }
    }
    
    private void process(
        final KQuery kQuery
    ) {
        final KQueryGenericData subQuery = kQuery.generateSubQueryData();
        
        this.kQueryInsertIntoData.params.addAll(subQuery.params);
        this.kQueryInsertIntoData.sb.append(" ").append(subQuery.sb);
    }
    
    public int execute() {
        return super.executeSingle();
    }
}

