package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToSelect;
import java.util.ArrayList;

public class KWith extends KQuery {

    private KWith(
        final KExecutor kExecutor
    ) {
        super(kExecutor, new ArrayList<>());
    }

    protected static KWith getInstance(
        final KExecutor kExecutor,
        final boolean recursive,
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        final KWith kWith = new KWith(kExecutor);

        kWith.process(recursive, kCommonTableExpressionsFilled);

        return kWith;
    }
    
    public KDeleteFrom deleteFrom(
        final KTable kTable
    ) {
        return KDeleteFrom.getInstance(
            this.k,
            new KQueryDeleteData(new StringBuilder(this.kQueryData.sb), new ArrayList<>(this.kQueryData.params), new ArrayList<>(), new ArrayList<>(), 0, new ArrayList()),
            kTable
        );
    }
    
    public KDeleteFrom deleteFrom(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return KDeleteFrom.getInstance(
            this.k,
            new KQueryDeleteData(new StringBuilder(this.kQueryData.sb), new ArrayList<>(this.kQueryData.params), new ArrayList<>(), new ArrayList<>(), 0, new ArrayList()),
            new KTable(kRaw.content, new KQueryData(kRaw.params))
        );
    }
    
    public KInsertInto insertInto(
        final KTable kTable
    ) {
        return KInsertInto.getInstance(
            this.k,
            new KQueryInsertIntoData(new StringBuilder(this.kQueryData.sb), new ArrayList<>(this.kQueryData.params), new ArrayList<>(), new ArrayList<>(), 0, new ArrayList()),
            kTable
        );
    }
    
    public KSelect select(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kColumnsAllowedToSelect);
    }
    
    public KSelect select(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQuery, alias);
    }
    
    public KSelect selectDistinct(
        final KColumnAllowedToSelect... kColumnsAllowedToSelect
    ) {
        return KSelect.getDistinctInstance(this.k, this.kSpecialFunctions, this.kQueryData, kColumnsAllowedToSelect);
    }
    
    public KSelect selectDistinct(
        final KQuery kQuery,
        final String alias
    ) {
        return KSelect.getDistinctInstance(this.k, this.kSpecialFunctions, this.kQueryData, kQuery, alias);
    }
    
    public KSelect select1() {
        return KSelect.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, KFunction.val(1));
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KColumn kColumn
    ) {
        return KDistinctOnSelect.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kColumn);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final int n
    ) {
        return KDistinctOnSelect.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, n);
    }
    
    public KDistinctOnSelect selectDistinctOn(
        final KRaw kRaw
    ) {
        return KDistinctOnSelect.getInstance(this.k, this.kSpecialFunctions, this.kQueryData, kRaw);
    }
    
    public KUpdate update(
        final KTable kTable
    ) {
        return KUpdate.getInstance(
            this.k,
            new KQueryUpdateData(new StringBuilder(this.kQueryData.sb), new ArrayList<>(this.kQueryData.params), new ArrayList<>(), new ArrayList<>(), 0, 0, new ArrayList()),
            kTable
        );
    }
    
    public KUpdate update(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return KUpdate.getInstance(
            this.k,
            new KQueryUpdateData(new StringBuilder(this.kQueryData.sb), new ArrayList<>(this.kQueryData.params), new ArrayList<>(), new ArrayList<>(), 0, 0, new ArrayList()),
            new KTable(kRaw.content, new KQueryData(kRaw.params))
        );
    }
    
    private void process(
        final boolean recursive,
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        KQueryUtils.processWith(
            this.kQueryData, 
            recursive,
            kCommonTableExpressionsFilled
        );
    }
    
}