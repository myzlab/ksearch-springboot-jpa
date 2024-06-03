package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import java.util.ArrayList;
import java.util.List;

public class KValNumberField extends KBaseValField implements KColumnAllowedToSetUpdate, KColumnAllowedToSelect, KColumnAllowedToReturning {
    
    protected KValNumberField() {
        super();
    }
    
    protected KValNumberField(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KValNumberField(
        final Number val
    ) {
        super(val);
    }
    
    public KColumn add(
        final KColumn kColumn
    ) {
        return KFunction.add(this, kColumn);
    }
    
    public KValNumberField add(
        final KValNumberField kValNumberField
    ) {
        return KFunction.add(this, kValNumberField);
    }
    
    public KValNumberField add(
        final Number number
    ) {
        return KFunction.add(this, number);
    }
    
    public KColumn divide(
        final KColumn kColumn
    ) {
        return KFunction.divide(this, kColumn);
    }
    
    public KValNumberField divide(
        final KValNumberField kValNumberField
    ) {
        return KFunction.divide(this, kValNumberField);
    }
    
    public KValNumberField divide(
        final Number number
    ) {
        return KFunction.divide(this, number);
    }
    
    public KColumn subtract(
        final KColumn kColumn
    ) {
        return KFunction.subtract(this, kColumn);
    }
    
    public KValNumberField subtract(
        final KValNumberField kValNumberField
    ) {
        return KFunction.subtract(this, kValNumberField);
    }
    
    public KValNumberField subtract(
        final Number number
    ) {
        return KFunction.subtract(this, new KValNumberField(number));
    }
    
    public KColumn mod(
        final KColumn kColumn
    ) {
        return KFunction.mod(this, kColumn);
    }
    
    public KValNumberField mod(
        final KValNumberField kValNumberField
    ) {
        return KFunction.mod(this, kValNumberField);
    }
    
    public KValNumberField mod(
        final Number number
    ) {
        return KFunction.mod(this, new KValNumberField(number));
    }
    
    public KColumn multiply(
        final KColumn kColumn
    ) {
        return KFunction.multiply(this, kColumn);
    }
    
    public KValNumberField multiply(
        final KValNumberField kValNumberField
    ) {
        return KFunction.multiply(this, kValNumberField);
    }
    
    public KValNumberField multiply(
        final Number number
    ) {
        return KFunction.multiply(this, new KValNumberField(number));
    }
    
    public KColumnOrdered asc() {
        return new KColumnOrdered(sb, params, closed, 1);
    }
    
    public KColumnOrdered desc() {
        return new KColumnOrdered(sb, params, closed, -1);
    }
    
    @Override
    protected KValNumberField cloneMe() {
        return new KValNumberField(this.sb, new ArrayList<>(this.params), this.closed);
    }

    @Override
    public String getSqlToSet() {
        return this.sb.toString();
    }

    @Override
    public List<Object> getParams() {
        return this.params;
    }
    
    @Override
    public KBaseColumn getKBaseColumn() {
        return this;
    }
    
    @Override
    public String getSqlToReturning() {
        return sb.toString();
    }
}
