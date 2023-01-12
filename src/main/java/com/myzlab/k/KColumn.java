package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToGroupBy;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.optional.KOptionalKColumn;
import com.myzlab.k.optional.KOptionalKValTextField;
import com.myzlab.k.optional.KOptionalString;
import java.util.ArrayList;
import java.util.List;

public class KColumn extends KBaseColumnCastable implements 
    TextMethods,
    KColumnAllowedToOrderBy,
    KColumnAllowedToGroupBy,
    KColumnAllowedToReturning,
    KColumnAllowedToSetUpdate,
    KColumnAllowedToSelect 
{
    
    protected KColumn() {
        super();
    }
    
    protected KColumn(
        final boolean closed
    ) {
        super(closed);
    }
    
    protected KColumn(
        final KTable kTable,
        final String name,
        final Class type
    ) {
        super(true, name, type, kTable);
        
        this.sb.append(kTable.alias).append(".").append(name);
    }
    
    protected KColumn(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KColumn(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable
    ) {
        super(sb, params, closed, name, type, kTable);
    }
    
    protected KColumn(
        final StringBuilder sb,
        final boolean closed
    ) {
        super(sb, new ArrayList<>(), closed);
    }
    
    protected KColumn(
        final String val
    ) {
        super(val);
    }
    
    protected KColumn(
        final Number val
    ) {
        super(val);
    }
    
//    protected KColumn(
//        final KTable kTable,
//        final StringBuilder sb,
//        final String name,
//        final Class type
//    ) {
//        this(kTable, name, type);
//        this.sb.append(sb);
//    }
    
    public KColumn add(
        final KColumn kColumn
    ) {
        return KFunction.add(this, kColumn);
    }
    
    public KColumn add(
        final KValNumberField kValNumberField
    ) {
        return KFunction.add(this, kValNumberField);
    }
    
    public KColumn add(
        final Number number
    ) {
        return KFunction.add(this, number);
    }
    
    public KColumn avg() {
        return KFunction.avg(this);
    }
    
    public KColumn count() {
        return KFunction.count(this);
    }
    
    public KColumn countDistinct() {
        return KFunction.countDistinct(this);
    }
    
    public KColumn div(
        final KColumn kColumn
    ) {
        return KFunction.div(this, kColumn);
    }
    
    public KColumn div(
        final KValNumberField kValNumberField
    ) {
        return KFunction.div(this, kValNumberField);
    }
    
    public KColumn div(
        final Number number
    ) {
        return KFunction.div(this, number);
    }
    
    public KColumn excluded() {
        return KFunction.excluded(this);
    }
    
    public KColumn grouping() {
        return KFunction.grouping(this);
    }
    
    public KColumn tableNameAlias() {
        return KFunction.tableNameAlias(this);
    }
    
    public KColumn noTableAlias() {
        return KFunction.noTableAlias(this);
    }
    
    @Override
    public KCondition iBetween(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    ) {
        return KCondition.ibt(this, kColumnLow, kColumnHigh);
    }
    
    @Override
    public KCondition iBetween(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    ) {
        return KCondition.ibt(this, kValTextFieldLow, kValTextFieldHigh);
    }
    
    @Override
    public KCondition iBetween(
        final String valueLow,
        final String valueHigh
    ) {
        return KCondition.ibt(this, KFunction.val(valueLow), KFunction.val(valueHigh));
    }
    
    @Override
    public KCondition ibt(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    ) {
        return KCondition.ibt(this, kColumnLow, kColumnHigh);
    }
    
    @Override
    public KCondition ibt(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    ) {
        return KCondition.ibt(this, kValTextFieldLow, kValTextFieldHigh);
    }
    
    @Override
    public KCondition ibt(
        final String valueLow,
        final String valueHigh
    ) {
        return KCondition.ibt(this, KFunction.val(valueLow), KFunction.val(valueHigh));
    }
    
    @Override
    public KCondition ieq(
        final KColumn kColumn
    ) {
        return KCondition.ieq(this, kColumn);
    }
    
    @Override
    public KCondition ieq(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ieq(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition ieq(
        final KValTextField kValTextField
    ) {
        return KCondition.ieq(this, kValTextField);
    }
    
    @Override
    public KCondition ieq(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ieq(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition ieq(
        final String value
    ) {
        return KCondition.ieq(this, KFunction.val(value));
    }
    
    @Override
    public KCondition ieq(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ieq(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition iEqual(
        final KColumn kColumn
    ) {
        return KCondition.ieq(this, kColumn);
    }
    
    @Override
    public KCondition iEqual(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ieq(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition iEqual(
        final KValTextField kValTextField
    ) {
        return KCondition.ieq(this, kValTextField);
    }
    
    @Override
    public KCondition iEqual(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ieq(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition iEqual(
        final String value
    ) {
        return KCondition.ieq(this, KFunction.val(value));
    }
    
    @Override
    public KCondition iEqual(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ieq(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition igt(
        final KColumn kColumn
    ) {
        return KCondition.igt(this, kColumn);
    }
    
    @Override
    public KCondition igt(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.igt(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition igt(
        final KValTextField kValTextField
    ) {
        return KCondition.igt(this, kValTextField);
    }
    
    @Override
    public KCondition igt(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.igt(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition igt(
        final String value
    ) {
        return KCondition.igt(this, KFunction.val(value));
    }
    
    @Override
    public KCondition igt(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.igt(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition igte(
        final KColumn kColumn
    ) {
        return KCondition.igte(this, kColumn);
    }
    
    @Override
    public KCondition igte(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.igte(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition igte(
        final KValTextField kValTextField
    ) {
        return KCondition.igte(this, kValTextField);
    }
    
    @Override
    public KCondition igte(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.igte(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition igte(
        final String value
    ) {
        return KCondition.igte(this, KFunction.val(value));
    }
    
    @Override
    public KCondition igte(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.igte(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition iGreaterThan(
        final KColumn kColumn
    ) {
        return KCondition.igt(this, kColumn);
    }
    
    @Override
    public KCondition iGreaterThan(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.igt(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition iGreaterThan(
        final KValTextField kValTextField
    ) {
        return KCondition.igt(this, kValTextField);
    }
    
    @Override
    public KCondition iGreaterThan(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.igt(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition iGreaterThan(
        final String value
    ) {
        return KCondition.igt(this, KFunction.val(value));
    }
    
    @Override
    public KCondition iGreaterThan(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.igt(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition iGreaterThanOrEqualTo(
        final KColumn kColumn
    ) {
        return KCondition.igte(this, kColumn);
    }
    
    @Override
    public KCondition iGreaterThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.igte(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition iGreaterThanOrEqualTo(
        final KValTextField kValTextField
    ) {
        return KCondition.igte(this, kValTextField);
    }
    
    @Override
    public KCondition iGreaterThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.igte(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition iGreaterThanOrEqualTo(
        final String value
    ) {
        return KCondition.igte(this, KFunction.val(value));
    }
    
    @Override
    public KCondition iGreaterThanOrEqualTo(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.igte(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition iLessThan(
        final KColumn kColumn
    ) {
        return KCondition.ilt(this, kColumn);
    }
    
    @Override
    public KCondition iLessThan(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilt(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition iLessThan(
        final KValTextField kValTextField
    ) {
        return KCondition.ilt(this, kValTextField);
    }
    
    @Override
    public KCondition iLessThan(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilt(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition iLessThan(
        final String value
    ) {
        return KCondition.ilt(this, KFunction.val(value));
    }
    
    @Override
    public KCondition iLessThan(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilt(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition iLessThanOrEqualTo(
        final KColumn kColumn
    ) {
        return KCondition.ilte(this, kColumn);
    }
    
    @Override
    public KCondition iLessThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilte(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition iLessThanOrEqualTo(
        final KValTextField kValTextField
    ) {
        return KCondition.ilte(this, kValTextField);
    }
    
    @Override
    public KCondition iLessThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilte(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition iLessThanOrEqualTo(
        final String value
    ) {
        return KCondition.ilte(this, KFunction.val(value));
    }
    
    @Override
    public KCondition iLessThanOrEqualTo(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilte(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition iLike(
        final KColumn kColumn
    ) {
        return KCondition.ilk(this, kColumn);
    }
    
    @Override
    public KCondition iLike(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilk(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition iLike(
        final KValTextField kValTextField
    ) {
        return KCondition.ilk(this, kValTextField);
    }
    
    @Override
    public KCondition iLike(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilk(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition iLike(
        final String value
    ) {
        return KCondition.ilk(this, KFunction.val(value));
    }
    
    @Override
    public KCondition iLike(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilk(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition iLikeAny(
        final KColumn kColumn
    ) {
        return KCondition.ilka(this, kColumn);
    }
    
    @Override
    public KCondition iLikeAny(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilka(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition iLikeAny(
        final KValTextField kValTextField
    ) {
        return KCondition.ilka(this, kValTextField);
    }
    
    @Override
    public KCondition iLikeAny(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilka(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition iLikeAny(
        final String value
    ) {
        return KCondition.ilka(this, KFunction.val(value));
    }
    
    @Override
    public KCondition iLikeAny(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilka(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition iLikeEndWith(
        final KColumn kColumn
    ) {
        return KCondition.ilkew(this, kColumn);
    }
    
    @Override
    public KCondition iLikeEndWith(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilkew(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition iLikeEndWith(
        final KValTextField kValTextField
    ) {
        return KCondition.ilkew(this, kValTextField);
    }
    
    @Override
    public KCondition iLikeEndWith(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilkew(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition iLikeEndWith(
        final String value
    ) {
        return KCondition.ilkew(this, KFunction.val(value));
    }
    
    @Override
    public KCondition iLikeEndWith(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilkew(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition iLikeStartWith(
        final KColumn kColumn
    ) {
        return KCondition.ilksw(this, kColumn);
    }
    
    @Override
    public KCondition iLikeStartWith(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilksw(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition iLikeStartWith(
        final KValTextField kValTextField
    ) {
        return KCondition.ilksw(this, kValTextField);
    }
    
    @Override
    public KCondition iLikeStartWith(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilksw(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition iLikeStartWith(
        final String value
    ) {
        return KCondition.ilksw(this, KFunction.val(value));
    }
    
    @Override
    public KCondition iLikeStartWith(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilksw(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition ilk(
        final KColumn kColumn
    ) {
        return KCondition.ilk(this, kColumn);
    }
    
    @Override
    public KCondition ilk(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilk(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition ilk(
        final KValTextField kValTextField
    ) {
        return KCondition.ilk(this, kValTextField);
    }
    
    @Override
    public KCondition ilk(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilk(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition ilk(
        final String value
    ) {
        return KCondition.ilk(this, KFunction.val(value));
    }
    
    @Override
    public KCondition ilk(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilk(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition ilka(
        final KColumn kColumn
    ) {
        return KCondition.ilka(this, kColumn);
    }
    
    @Override
    public KCondition ilka(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilka(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition ilka(
        final KValTextField kValTextField
    ) {
        return KCondition.ilka(this, kValTextField);
    }
    
    @Override
    public KCondition ilka(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilka(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition ilka(
        final String value
    ) {
        return KCondition.ilka(this, KFunction.val(value));
    }
    
    @Override
    public KCondition ilka(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilka(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition ilkew(
        final KColumn kColumn
    ) {
        return KCondition.ilkew(this, kColumn);
    }
    
    @Override
    public KCondition ilkew(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilkew(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition ilkew(
        final KValTextField kValTextField
    ) {
        return KCondition.ilkew(this, kValTextField);
    }
    
    @Override
    public KCondition ilkew(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilkew(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition ilkew(
        final String value
    ) {
        return KCondition.ilkew(this, KFunction.val(value));
    }
    
    @Override
    public KCondition ilkew(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilkew(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition ilksw(
        final KColumn kColumn
    ) {
        return KCondition.ilksw(this, kColumn);
    }
    
    @Override
    public KCondition ilksw(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilksw(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition ilksw(
        final KValTextField kValTextField
    ) {
        return KCondition.ilksw(this, kValTextField);
    }
    
    @Override
    public KCondition ilksw(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilksw(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition ilksw(
        final String value
    ) {
        return KCondition.ilksw(this, KFunction.val(value));
    }
    
    @Override
    public KCondition ilksw(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilksw(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition ilt(
        final KColumn kColumn
    ) {
        return KCondition.ilt(this, kColumn);
    }
    
    @Override
    public KCondition ilt(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilt(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition ilt(
        final KValTextField kValTextField
    ) {
        return KCondition.ilt(this, kValTextField);
    }
    
    @Override
    public KCondition ilt(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilt(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition ilt(
        final String value
    ) {
        return KCondition.ilt(this, KFunction.val(value));
    }
    
    @Override
    public KCondition ilt(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilt(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition ilte(
        final KColumn kColumn
    ) {
        return KCondition.ilte(this, kColumn);
    }
    
    @Override
    public KCondition ilte(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.ilte(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition ilte(
        final KValTextField kValTextField
    ) {
        return KCondition.ilte(this, kValTextField);
    }
    
    @Override
    public KCondition ilte(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilte(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition ilte(
        final String value
    ) {
        return KCondition.ilte(this, KFunction.val(value));
    }
    
    @Override
    public KCondition ilte(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.ilte(this, KFunction.val(kOptionalString.get()));
    }
    
    public KColumn isolate() {
        return KFunction.isolate(this);
    }
    
    public KColumn getJsonArray(final int index) {
        return KFunction.getJsonArray(this, index);
    }
    
    public KColumn getJsonArrayAsText(final int index) {
        return KFunction.getJsonArrayAsText(this, index);
    }
    
    public KColumn getJsonObject(final String name) {
        return KFunction.getJsonObject(this, name);
    }
    
    public KColumn getJsonObjectAsText(final String name) {
        return KFunction.getJsonObjectAsText(this, name);
    }
    
    public KColumn getJsonObjectAtPath(final String name) {
        return KFunction.getJsonObjectAtPath(this, name);
    }
    
    public KColumn getJsonObjectAtPathAsText(final String name) {
        return KFunction.getJsonObjectAtPathAsText(this, name);
    }
    
    @Override
    public KCondition like(
        final KColumn kColumn
    ) {
        return KCondition.lk(this, kColumn);
    }
    
    @Override
    public KCondition like(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lk(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition like(
        final KValTextField kValTextField
    ) {
        return KCondition.lk(this, kValTextField);
    }
    
    @Override
    public KCondition like(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lk(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition like(
        final String value
    ) {
        return KCondition.lk(this, KFunction.val(value));
    }
    
    @Override
    public KCondition like(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lk(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition likeAny(
        final KColumn kColumn
    ) {
        return KCondition.lka(this, kColumn);
    }
    
    @Override
    public KCondition likeAny(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lka(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition likeAny(
        final KValTextField kValTextField
    ) {
        return KCondition.lka(this, kValTextField);
    }
    
    @Override
    public KCondition likeAny(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lka(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition likeAny(
        final String value
    ) {
        return KCondition.lka(this, KFunction.val(value));
    }
    
    @Override
    public KCondition likeAny(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lka(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition likeEndWith(
        final KColumn kColumn
    ) {
        return KCondition.lkew(this, kColumn);
    }
    
    @Override
    public KCondition likeEndWith(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lkew(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition likeEndWith(
        final KValTextField kValTextField
    ) {
        return KCondition.lkew(this, kValTextField);
    }
    
    @Override
    public KCondition likeEndWith(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lkew(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition likeEndWith(
        final String value
    ) {
        return KCondition.lkew(this, KFunction.val(value));
    }
    
    @Override
    public KCondition likeEndWith(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lkew(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition likeStartWith(
        final KColumn kColumn
    ) {
        return KCondition.lksw(this, kColumn);
    }
    
    @Override
    public KCondition likeStartWith(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lksw(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition likeStartWith(
        final KValTextField kValTextField
    ) {
        return KCondition.lksw(this, kValTextField);
    }
    
    @Override
    public KCondition likeStartWith(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lksw(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition likeStartWith(
        final String value
    ) {
        return KCondition.lksw(this, KFunction.val(value));
    }
    
    @Override
    public KCondition likeStartWith(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lksw(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition lk(
        final KColumn kColumn
    ) {
        return KCondition.lk(this, kColumn);
    }
    
    @Override
    public KCondition lk(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lk(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition lk(
        final KValTextField kValTextField
    ) {
        return KCondition.lk(this, kValTextField);
    }
    
    @Override
    public KCondition lk(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lk(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition lk(
        final String value
    ) {
        return KCondition.lk(this, KFunction.val(value));
    }
    
    @Override
    public KCondition lk(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lk(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition lka(
        final KColumn kColumn
    ) {
        return KCondition.lka(this, kColumn);
    }
    
    @Override
    public KCondition lka(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lka(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition lka(
        final KValTextField kValTextField
    ) {
        return KCondition.lka(this, kValTextField);
    }
    
    @Override
    public KCondition lka(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lka(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition lka(
        final String value
    ) {
        return KCondition.lka(this, KFunction.val(value));
    }
    
    @Override
    public KCondition lka(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lka(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition lkew(
        final KColumn kColumn
    ) {
        return KCondition.lkew(this, kColumn);
    }
    
    @Override
    public KCondition lkew(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lkew(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition lkew(
        final KValTextField kValTextField
    ) {
        return KCondition.lkew(this, kValTextField);
    }
    
    @Override
    public KCondition lkew(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lkew(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition lkew(
        final String value
    ) {
        return KCondition.lkew(this, KFunction.val(value));
    }
    
    @Override
    public KCondition lkew(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lkew(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition lksw(
        final KColumn kColumn
    ) {
        return KCondition.lksw(this, kColumn);
    }
    
    @Override
    public KCondition lksw(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.lksw(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition lksw(
        final KValTextField kValTextField
    ) {
        return KCondition.lksw(this, kValTextField);
    }
    
    @Override
    public KCondition lksw(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lksw(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition lksw(
        final String value
    ) {
        return KCondition.lksw(this, KFunction.val(value));
    }
    
    @Override
    public KCondition lksw(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.lksw(this, KFunction.val(kOptionalString.get()));
    }
    
    public KColumn max() {
        return KFunction.max(this);
    }
    
    public KColumn min() {
        return KFunction.min(this);
    }
    
    public KColumn mod(
        final KColumn kColumn
    ) {
        return KFunction.mod(this, kColumn);
    }
    
    public KColumn mod(
        final KValNumberField kValNumberField
    ) {
        return KFunction.mod(this, kValNumberField);
    }
    
    public KColumn mod(
        final Number number
    ) {
        return KFunction.mod(this, number);
    }
    
    public KColumn mul(
        final KColumn kColumn
    ) {
        return KFunction.mul(this, kColumn);
    }
    
    public KColumn mul(
        final KValNumberField kValNumberField
    ) {
        return KFunction.mul(this, kValNumberField);
    }
    
    public KColumn mul(
        final Number number
    ) {
        return KFunction.mul(this, number);
    }
    
    @Override
    public KCondition nilk(
        final KColumn kColumn
    ) {
        return KCondition.nilk(this, kColumn);
    }
    
    @Override
    public KCondition nilk(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilk(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nilk(
        final KValTextField kValTextField
    ) {
        return KCondition.nilk(this, kValTextField);
    }
    
    @Override
    public KCondition nilk(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilk(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nilk(
        final String value
    ) {
        return KCondition.nilk(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nilk(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilk(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition nilka(
        final KColumn kColumn
    ) {
        return KCondition.nilka(this, kColumn);
    }
    
    @Override
    public KCondition nilka(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilka(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nilka(
        final KValTextField kValTextField
    ) {
        return KCondition.nilka(this, kValTextField);
    }
    
    @Override
    public KCondition nilka(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilka(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nilka(
        final String value
    ) {
        return KCondition.nilka(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nilka(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilka(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition nilkew(
        final KColumn kColumn
    ) {
        return KCondition.nilkew(this, kColumn);
    }
    
    @Override
    public KCondition nilkew(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilkew(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nilkew(
        final KValTextField kValTextField
    ) {
        return KCondition.nilkew(this, kValTextField);
    }
    
    @Override
    public KCondition nilkew(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilkew(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nilkew(
        final String value
    ) {
        return KCondition.nilkew(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nilkew(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilkew(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition nilksw(
        final KColumn kColumn
    ) {
        return KCondition.nilksw(this, kColumn);
    }
    
    @Override
    public KCondition nilksw(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilksw(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nilksw(
        final KValTextField kValTextField
    ) {
        return KCondition.nilksw(this, kValTextField);
    }
    
    @Override
    public KCondition nilksw(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilksw(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nilksw(
        final String value
    ) {
        return KCondition.nilksw(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nilksw(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilksw(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notIBetween(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    ) {
        return KCondition.nibt(this, kColumnLow, kColumnHigh);
    }
    
    @Override
    public KCondition notIBetween(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    ) {
        return KCondition.nibt(this, kValTextFieldLow, kValTextFieldHigh);
    }
    
    @Override
    public KCondition notIBetween(
        final String valueLow,
        final String valueHigh
    ) {
        return KCondition.nibt(this, KFunction.val(valueLow), KFunction.val(valueHigh));
    }
    
    @Override
    public KCondition nibt(
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    ) {
        return KCondition.nibt(this, kColumnLow, kColumnHigh);
    }
    
    @Override
    public KCondition nibt(
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    ) {
        return KCondition.nibt(this, kValTextFieldLow, kValTextFieldHigh);
    }
    
    @Override
    public KCondition nibt(
        final String valueLow,
        final String valueHigh
    ) {
        return KCondition.nibt(this, KFunction.val(valueLow), KFunction.val(valueHigh));
    }
    
    @Override
    public KCondition nieq(
        final KColumn kColumn
    ) {
        return KCondition.nieq(this, kColumn);
    }
    
    @Override
    public KCondition nieq(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nieq(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nieq(
        final KValTextField kValTextField
    ) {
        return KCondition.nieq(this, kValTextField);
    }
    
    @Override
    public KCondition nieq(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nieq(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nieq(
        final String value
    ) {
        return KCondition.nieq(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nieq(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nieq(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notIEqual(
        final KColumn kColumn
    ) {
        return KCondition.nieq(this, kColumn);
    }
    
    @Override
    public KCondition notIEqual(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nieq(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notIEqual(
        final KValTextField kValTextField
    ) {
        return KCondition.nieq(this, kValTextField);
    }
    
    @Override
    public KCondition notIEqual(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nieq(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notIEqual(
        final String value
    ) {
        return KCondition.nieq(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notIEqual(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nieq(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition nigt(
        final KColumn kColumn
    ) {
        return KCondition.nigt(this, kColumn);
    }
    
    @Override
    public KCondition nigt(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nigt(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nigt(
        final KValTextField kValTextField
    ) {
        return KCondition.nigt(this, kValTextField);
    }
    
    @Override
    public KCondition nigt(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nigt(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nigt(
        final String value
    ) {
        return KCondition.nigt(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nigt(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nigt(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition nigte(
        final KColumn kColumn
    ) {
        return KCondition.nigte(this, kColumn);
    }
    
    @Override
    public KCondition nigte(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nigte(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nigte(
        final KValTextField kValTextField
    ) {
        return KCondition.nigte(this, kValTextField);
    }
    
    @Override
    public KCondition nigte(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nigte(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nigte(
        final String value
    ) {
        return KCondition.nigte(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nigte(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nigte(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition nilt(
        final KColumn kColumn
    ) {
        return KCondition.nilt(this, kColumn);
    }
    
    @Override
    public KCondition nilt(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilt(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nilt(
        final KValTextField kValTextField
    ) {
        return KCondition.nilt(this, kValTextField);
    }
    
    @Override
    public KCondition nilt(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilt(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nilt(
        final String value
    ) {
        return KCondition.nilt(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nilt(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilt(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition nilte(
        final KColumn kColumn
    ) {
        return KCondition.nilte(this, kColumn);
    }
    
    @Override
    public KCondition nilte(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilte(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nilte(
        final KValTextField kValTextField
    ) {
        return KCondition.nilte(this, kValTextField);
    }
    
    @Override
    public KCondition nilte(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilte(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nilte(
        final String value
    ) {
        return KCondition.nilte(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nilte(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilte(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notIGreaterThan(
        final KColumn kColumn
    ) {
        return KCondition.nigt(this, kColumn);
    }
    
    @Override
    public KCondition notIGreaterThan(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nigt(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notIGreaterThan(
        final KValTextField kValTextField
    ) {
        return KCondition.nigt(this, kValTextField);
    }
    
    @Override
    public KCondition notIGreaterThan(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nigt(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notIGreaterThan(
        final String value
    ) {
        return KCondition.nigt(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notIGreaterThan(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nigt(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notIGreaterThanOrEqualTo(
        final KColumn kColumn
    ) {
        return KCondition.nigte(this, kColumn);
    }
    
    @Override
    public KCondition notIGreaterThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nigte(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notIGreaterThanOrEqualTo(
        final KValTextField kValTextField
    ) {
        return KCondition.nigte(this, kValTextField);
    }
    
    @Override
    public KCondition notIGreaterThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nigte(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notIGreaterThanOrEqualTo(
        final String value
    ) {
        return KCondition.nigte(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notIGreaterThanOrEqualTo(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nigte(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notILessThan(
        final KColumn kColumn
    ) {
        return KCondition.nilt(this, kColumn);
    }
    
    @Override
    public KCondition notILessThan(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilt(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notILessThan(
        final KValTextField kValTextField
    ) {
        return KCondition.nilt(this, kValTextField);
    }
    
    @Override
    public KCondition notILessThan(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilt(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notILessThan(
        final String value
    ) {
        return KCondition.nilt(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notILessThan(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilt(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notILessThanOrEqualTo(
        final KColumn kColumn
    ) {
        return KCondition.nilte(this, kColumn);
    }
    
    @Override
    public KCondition notILessThanOrEqualTo(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilte(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notILessThanOrEqualTo(
        final KValTextField kValTextField
    ) {
        return KCondition.nilte(this, kValTextField);
    }
    
    @Override
    public KCondition notILessThanOrEqualTo(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilte(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notILessThanOrEqualTo(
        final String value
    ) {
        return KCondition.nilte(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notILessThanOrEqualTo(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilte(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notILike(
        final KColumn kColumn
    ) {
        return KCondition.nilk(this, kColumn);
    }
    
    @Override
    public KCondition notILike(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilk(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notILike(
        final KValTextField kValTextField
    ) {
        return KCondition.nilk(this, kValTextField);
    }
    
    @Override
    public KCondition notILike(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilk(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notILike(
        final String value
    ) {
        return KCondition.nilk(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notILike(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilk(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notILikeAny(
        final KColumn kColumn
    ) {
        return KCondition.nilka(this, kColumn);
    }
    
    @Override
    public KCondition notILikeAny(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilka(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notILikeAny(
        final KValTextField kValTextField
    ) {
        return KCondition.nilka(this, kValTextField);
    }
    
    @Override
    public KCondition notILikeAny(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilka(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notILikeAny(
        final String value
    ) {
        return KCondition.nilka(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notILikeAny(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilka(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notILikeEndWith(
        final KColumn kColumn
    ) {
        return KCondition.nilkew(this, kColumn);
    }
    
    @Override
    public KCondition notILikeEndWith(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilkew(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notILikeEndWith(
        final KValTextField kValTextField
    ) {
        return KCondition.nilkew(this, kValTextField);
    }
    
    @Override
    public KCondition notILikeEndWith(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilkew(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notILikeEndWith(
        final String value
    ) {
        return KCondition.nilkew(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notILikeEndWith(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilkew(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notILikeStartWith(
        final KColumn kColumn
    ) {
        return KCondition.nilksw(this, kColumn);
    }
    
    @Override
    public KCondition notILikeStartWith(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nilksw(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notILikeStartWith(
        final KValTextField kValTextField
    ) {
        return KCondition.nilksw(this, kValTextField);
    }
    
    @Override
    public KCondition notILikeStartWith(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilksw(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notILikeStartWith(
        final String value
    ) {
        return KCondition.nilksw(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notILikeStartWith(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nilksw(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition nlk(
        final KColumn kColumn
    ) {
        return KCondition.nlk(this, kColumn);
    }
    
    @Override
    public KCondition nlk(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlk(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nlk(
        final KValTextField kValTextField
    ) {
        return KCondition.nlk(this, kValTextField);
    }
    
    @Override
    public KCondition nlk(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlk(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nlk(
        final String value
    ) {
        return KCondition.nlk(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nlk(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlk(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition nlka(
        final KColumn kColumn
    ) {
        return KCondition.nlka(this, kColumn);
    }
    
    @Override
    public KCondition nlka(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlka(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nlka(
        final KValTextField kValTextField
    ) {
        return KCondition.nlka(this, kValTextField);
    }
    
    @Override
    public KCondition nlka(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlka(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nlka(
        final String value
    ) {
        return KCondition.nlka(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nlka(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlka(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition nlkew(
        final KColumn kColumn
    ) {
        return KCondition.nlkew(this, kColumn);
    }
    
    @Override
    public KCondition nlkew(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlkew(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nlkew(
        final KValTextField kValTextField
    ) {
        return KCondition.nlkew(this, kValTextField);
    }
    
    @Override
    public KCondition nlkew(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlkew(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nlkew(
        final String value
    ) {
        return KCondition.nlkew(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nlkew(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlkew(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition nlksw(
        final KColumn kColumn
    ) {
        return KCondition.nlksw(this, kColumn);
    }
    
    @Override
    public KCondition nlksw(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlksw(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition nlksw(
        final KValTextField kValTextField
    ) {
        return KCondition.nlksw(this, kValTextField);
    }
    
    @Override
    public KCondition nlksw(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlksw(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition nlksw(
        final String value
    ) {
        return KCondition.nlksw(this, KFunction.val(value));
    }
    
    @Override
    public KCondition nlksw(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlksw(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notLike(
        final KColumn kColumn
    ) {
        return KCondition.nlk(this, kColumn);
    }
    
    @Override
    public KCondition notLike(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlk(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notLike(
        final KValTextField kValTextField
    ) {
        return KCondition.nlk(this, kValTextField);
    }
    
    @Override
    public KCondition notLike(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlk(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notLike(
        final String value
    ) {
        return KCondition.nlk(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notLike(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlk(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notLikeAny(
        final KColumn kColumn
    ) {
        return KCondition.nlka(this, kColumn);
    }
    
    @Override
    public KCondition notLikeAny(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlka(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notLikeAny(
        final KValTextField kValTextField
    ) {
        return KCondition.nlka(this, kValTextField);
    }
    
    @Override
    public KCondition notLikeAny(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlka(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notLikeAny(
        final String value
    ) {
        return KCondition.nlka(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notLikeAny(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlka(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notLikeEndWith(
        final KColumn kColumn
    ) {
        return KCondition.nlkew(this, kColumn);
    }
    
    @Override
    public KCondition notLikeEndWith(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlkew(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notLikeEndWith(
        final KValTextField kValTextField
    ) {
        return KCondition.nlkew(this, kValTextField);
    }
    
    @Override
    public KCondition notLikeEndWith(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlkew(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notLikeEndWith(
        final String value
    ) {
        return KCondition.nlkew(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notLikeEndWith(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlkew(this, KFunction.val(kOptionalString.get()));
    }
    
    @Override
    public KCondition notLikeStartWith(
        final KColumn kColumn
    ) {
        return KCondition.nlksw(this, kColumn);
    }
    
    @Override
    public KCondition notLikeStartWith(
        final KOptionalKColumn kOptionalKColumn
    ) {
        if (!kOptionalKColumn.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return this.nlksw(kOptionalKColumn.get());
    }
    
    @Override
    public KCondition notLikeStartWith(
        final KValTextField kValTextField
    ) {
        return KCondition.nlksw(this, kValTextField);
    }
    
    @Override
    public KCondition notLikeStartWith(
        final KOptionalKValTextField kOptionalKValTextField
    ) {
        if (!kOptionalKValTextField.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlksw(this, kOptionalKValTextField.get());
    }
    
    @Override
    public KCondition notLikeStartWith(
        final String value
    ) {
        return KCondition.nlksw(this, KFunction.val(value));
    }
    
    @Override
    public KCondition notLikeStartWith(
        final KOptionalString kOptionalString
    ) {
        if (!kOptionalString.isPresent()) {
            return KCondition.getEmptyInstance();
        }
        
        return KCondition.nlksw(this, KFunction.val(kOptionalString.get()));
    }
    
    public KColumnOvered over(
        final KWindowDefinitionAllowedToOver kWindowDefinitionAllowedToOver
    ) {
        return new KColumnOvered(this.sb, this.params, this.closed, kWindowDefinitionAllowedToOver);
    }
    
    public KColumn sub(
        final KColumn kColumn
    ) {
        return KFunction.sub(this, kColumn);
    }
    
    public KColumn sub(
        final KValNumberField kValNumberField
    ) {
        return KFunction.sub(this, kValNumberField);
    }
    
    public KColumn sub(
        final Number number
    ) {
        return KFunction.sub(this, number);
    }
    
    public KColumn sum() {
        return KFunction.sum(this);
    }
    
    public KColumn sumDistinct() {
        return KFunction.sumDistinct(this);
    }
    
    public KColumnOrdered asc() {
        return new KColumnOrdered(this.sb, this.params, this.closed, 1);
    }
    
    public KColumnOrdered desc() {
        return new KColumnOrdered(this.sb, this.params, this.closed, -1);
    }
    
    @Override
    protected KColumn cloneMe() {
        return new KColumn(new StringBuilder(this.sb), new ArrayList<>(this.params), this.closed);
    }
    
    @Override
    public String getSqlToOrderBy() {
        return this.sb.toString();
    }
    
    @Override
    public String getSqlToGroupBy() {
        return this.sb.toString();
    }

    @Override
    public String getSqlToReturning() {
        return sb.toString();
    }

    @Override
    public List<Object> getParams() {
        return this.params;
    }

    @Override
    public String getSqlToSet() {
        return sb.toString();
    }

    @Override
    public KBaseColumn getKBaseColumn() {
        return this;
    }
    
}
