package com.myzlab.k;

public class KInsertIntoColumned extends KQueryInsertInto {

    private KInsertIntoColumned(
        final KExecutor kExecutor
    ) {
        super(kExecutor);
    }
 
    private KInsertIntoColumned(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KColumn... kColumns
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        this.process(kColumns);
    }
    
    protected static KInsertIntoColumned getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KColumn... kColumns
    ) {
        return new KInsertIntoColumned(kExecutor, kQueryInsertIntoData, kColumns);
    }
    
    public KInsertIntoFilled values(
        final KValues kValues
    ) {
        return KInsertIntoFilled.getInstance(this.k, this.kQueryInsertIntoData, kValues);
    }
    
    public KInsertIntoFilled select(
        final KQuery kQuery
    ) {
        return KInsertIntoFilled.getInstance(this.k, this.kQueryInsertIntoData, kQuery);
    }
    
    private void process(
        final KColumn... kColumns
    ) {
        KUtils.assertNotNullNotEmpty(kColumns, "kColumns", false);
            
        this.kQueryInsertIntoData.sb.append(" (");
        

        for (int i = 0; i < kColumns.length; i++) {
            final KColumn kColumn = kColumns[i];
                    
            if (i > 0) {
                this.kQueryInsertIntoData.sb.append(", ");
            }

            this.kQueryInsertIntoData.sb.append(kColumn.name);
        }

        this.kQueryInsertIntoData.sb.append(")");
    }
    
}

