package com.myzlab.k;

import com.myzlab.k.helper.KExceptionHelper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class KCondition {
    
    public final static int AND_TYPE = 1;
    public final static int OR_TYPE = 2;
    public final static int UNDEFINED_TYPE = 3;
    public final static int CLOSABLE_TYPE = 4;
    
    protected final StringBuilder sb;
    protected int operator = 0;
    protected List<Object> params;
    protected int type = UNDEFINED_TYPE;
    protected boolean emptyCondition = false;
    
    protected KCondition() {
        super();
        
        this.sb = new StringBuilder();
        this.params = new ArrayList<>();
        this.operator = 1;
    }
    
    protected KCondition(
        final String content
    ) {
        super();
        
        this.sb = new StringBuilder(content);
        this.params = new ArrayList<>();
        this.operator = 1;
    }
    
    protected KCondition(
        final int operator
    ) {
        super();
        
        this.sb = new StringBuilder();
        this.params = new ArrayList<>();
        this.operator = operator;
    }
    
    protected KCondition(
        final int operator,
        final boolean emptyCondition
    ) {
        this(operator);
        
        this.emptyCondition = emptyCondition;
    }
    
    public static KCondition getEmptyInstance() {
        return new KCondition(0, true);
    }
    
    public KCondition and(
        final KCondition kCondition
    ) {
        return applyLogicOperator(kCondition, "AND", AND_TYPE);
    }
    
    public KCondition and(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kCondition = new KCondition(kRaw.content);
        
        return applyLogicOperator(kCondition, "AND", AND_TYPE);
    }
    
    public KCondition andNot(
        final KCondition kCondition
    ) {
        return applyLogicOperator(KFunction.not(kCondition), "AND", AND_TYPE);
    }
    
    public KCondition andNot(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kCondition = new KCondition(kRaw.content);
        
        return applyLogicOperator(KFunction.not(kCondition), "AND", AND_TYPE);
    }
    
    public KAliasedColumn as(
        final String alias
    ) {
        KUtils.assertNotNull(alias, "alias");
        
        return new KAliasedColumn(new StringBuilder(this.sb), alias, this.params, false);
    }
    
    private KCondition applyLogicOperator(
        final KCondition kCondition,
        final String operator,
        final int type
    ) {
        
        if (this.emptyCondition) {
            this.sb.append(kCondition.sb);
            this.params.addAll(kCondition.params);
            this.operator = kCondition.operator;
            this.type = kCondition.type;
            this.emptyCondition = kCondition.emptyCondition;
            
            return this;
        }
        
        if (kCondition.emptyCondition) {
            return this;
        }
        
        if (this.type == CLOSABLE_TYPE || (this.type != UNDEFINED_TYPE && this.type != type)) {
            this.sb.insert(0, "(").append(")");
        }
        
        this.sb.append(" ").append(operator).append(" ");
        
        final boolean closeNext = this.type == CLOSABLE_TYPE || (kCondition.type != UNDEFINED_TYPE && kCondition.type != type);
        
        if (closeNext) {
            this.sb.append("(");
        }
        
        this.sb.append(kCondition.sb);
        
        if (closeNext) {
            this.sb.append(")");
        }
        
        this.params.addAll(kCondition.params);
        this.operator += kCondition.operator;
        this.type = type;
        
        return this;
    }
    
    protected static KCondition bt(
        final KBaseColumn kBaseColumnValue,
        final KBaseColumn kBaseColumnLow,
        final KBaseColumn kBaseColumnHigh
    ) {
        final KCondition kCondition = new KCondition();
        kCondition.type = CLOSABLE_TYPE;
        
        kCondition.params.addAll(kBaseColumnValue.params);
        kCondition.params.addAll(kBaseColumnLow.params);
        kCondition.params.addAll(kBaseColumnHigh.params);
        
        if (!kBaseColumnValue.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumnValue.sb);
        
        if (!kBaseColumnValue.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" BETWEEN ");
        
        if (!kBaseColumnLow.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumnLow.sb);
        
        if (!kBaseColumnLow.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" AND ");
        
        if (!kBaseColumnHigh.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumnHigh.sb);
        
        if (!kBaseColumnHigh.closed) {
            kCondition.sb.append(")");
        }
        
        return kCondition;
    }
    
    protected static KCondition eq(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processBinaryOperator(kBaseColumn1, kBaseColumn2, "=");
        
        return kCondition;
    }
    
    protected static KCondition eq(
        final KBaseColumn kBaseColumn1,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processBinaryOperator(kBaseColumn1, new KColumn(subQuery.sb, subQuery.params, false), "=");
        
        return kCondition;
    }
    
    protected static KCondition nlte(
        final KBaseColumn kBaseColumn1,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processNotBinaryOperator(kBaseColumn1, new KColumn(subQuery.sb, subQuery.params, false), "<=");
        
        return kCondition;
    }
    
    protected static KCondition gt(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processBinaryOperator(kBaseColumn1, kBaseColumn2, ">");
        
        return kCondition;
    }
    
    protected static KCondition gt(
        final KBaseColumn kBaseColumn1,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processBinaryOperator(kBaseColumn1, new KColumn(subQuery.sb, subQuery.params, false), ">");
        
        return kCondition;
    }
    
    protected static KCondition gte(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processBinaryOperator(kBaseColumn1, kBaseColumn2, ">=");
        
        return kCondition;
    }
    
    protected static KCondition gte(
        final KBaseColumn kBaseColumn1,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processBinaryOperator(kBaseColumn1, new KColumn(subQuery.sb, subQuery.params, false), ">=");
        
        return kCondition;
    }
    
    protected static KCondition lt(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processBinaryOperator(kBaseColumn1, kBaseColumn2, "<");
        
        return kCondition;
    }
    
    protected static KCondition lt(
        final KBaseColumn kBaseColumn1,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processBinaryOperator(kBaseColumn1, new KColumn(subQuery.sb, subQuery.params, false), "<");
        
        return kCondition;
    }
    
    protected static KCondition lte(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processBinaryOperator(kBaseColumn1, kBaseColumn2, "<=");
        
        return kCondition;
    }
    
    protected static KCondition lte(
        final KBaseColumn kBaseColumn1,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processBinaryOperator(kBaseColumn1, new KColumn(subQuery.sb, subQuery.params, false), "<=");
        
        return kCondition;
    }
    
    public KCondition or(
        final KCondition kCondition
    ) {
        return applyLogicOperator(kCondition, "OR", OR_TYPE);
    }
    
    public KCondition or(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kCondition = new KCondition(kRaw.content);
        
        return applyLogicOperator(kCondition, "OR", OR_TYPE);
    }
    
    public KCondition orNot(
        final KCondition kCondition
    ) {
        return applyLogicOperator(KFunction.not(kCondition), "OR", OR_TYPE);
    }
    
    public KCondition orNot(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kCondition = new KCondition(kRaw.content);
        
        return applyLogicOperator(KFunction.not(kCondition), "OR", OR_TYPE);
    }
    
    protected static KCondition ibt(
        final KBaseColumn kBaseColumnValue,
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    ) {
        final KCondition kCondition = new KCondition();
        kCondition.type = CLOSABLE_TYPE;
        
        for (final Object param : kBaseColumnValue.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kColumnLow.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kColumnHigh.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        if (kBaseColumnValue instanceof KValTextField) {
            if (!kBaseColumnValue.closed) {
                kCondition.sb.append("(");
            }
        } else {
            kCondition.sb.append("LOWER(");
        }
        
        kCondition.sb.append(kBaseColumnValue.sb);
        
        if (kBaseColumnValue instanceof KValTextField) {
            if (!kBaseColumnValue.closed) {
                kCondition.sb.append(")");
            }
        } else {
            kCondition.sb.append(")");
        }
        
        kCondition.sb
            .append(" BETWEEN ")
            .append("LOWER(").append(kColumnLow.sb).append(")")
            .append(" AND ")
            .append("LOWER(").append(kColumnHigh.sb).append(")");
        
        return kCondition;
    }
    
    protected static KCondition ibt(
        final KBaseColumn kBaseColumnValue,
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    ) {
        final KCondition kCondition = new KCondition();
        kCondition.type = CLOSABLE_TYPE;
        
        for (final Object param : kBaseColumnValue.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kValTextFieldLow.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kValTextFieldHigh.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        if (kBaseColumnValue instanceof KValTextField) {
            if (!kBaseColumnValue.closed) {
                kCondition.sb.append("(");
            }
        } else {
            kCondition.sb.append("LOWER(");
        }
        
        kCondition.sb.append(kBaseColumnValue.sb);
        
        if (kBaseColumnValue instanceof KValTextField) {
            if (!kBaseColumnValue.closed) {
                kCondition.sb.append(")");
            }
        } else {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" BETWEEN ");
        
        if (!kValTextFieldLow.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kValTextFieldLow.sb);
        
        if (!kValTextFieldLow.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" AND ");
        
        if (!kValTextFieldHigh.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kValTextFieldHigh.sb);
        
        if (!kValTextFieldHigh.closed) {
            kCondition.sb.append(")");
        }
        
        return kCondition;
    }
    
    protected static KCondition ieq(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn1, kColumn2, "=");
        
        return kCondition;
    }
    
    protected static KCondition ieq(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn, kValTextField, "=");
        
        return kCondition;
    }
    
    protected static KCondition ieq(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField, kColumn, "=");
        
        return kCondition;
    }
    
    protected static KCondition ieq(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField1, kValTextField2, "=");
        
        return kCondition;
    }
    
    protected static KCondition igt(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn1, kColumn2, ">");
        
        return kCondition;
    }
    
    protected static KCondition igt(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn, kValTextField, ">");
        
        return kCondition;
    }
    
    protected static KCondition igt(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField, kColumn, ">");
        
        return kCondition;
    }
    
    protected static KCondition igt(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField1, kValTextField2, ">");
        
        return kCondition;
    }
    
    protected static KCondition igte(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn1, kColumn2, ">=");
        
        return kCondition;
    }
    
    protected static KCondition igte(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn, kValTextField, ">=");
        
        return kCondition;
    }
    
    protected static KCondition igte(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField, kColumn, ">=");
        
        return kCondition;
    }
    
    protected static KCondition igte(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField1, kValTextField2, ">=");
        
        return kCondition;
    }
    
    protected static KCondition ilt(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn1, kColumn2, "<");
        
        return kCondition;
    }
    
    protected static KCondition ilt(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn, kValTextField, "<");
        
        return kCondition;
    }
    
    protected static KCondition ilt(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField, kColumn, "<");
        
        return kCondition;
    }
    
    protected static KCondition ilt(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField1, kValTextField2, "<");
        
        return kCondition;
    }
    
    protected static KCondition ilte(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn1, kColumn2, "<=");
        
        return kCondition;
    }
    
    protected static KCondition ilte(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn, kValTextField, "<=");
        
        return kCondition;
    }
    
    protected static KCondition ilte(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField, kColumn, "<=");
        
        return kCondition;
    }
    
    protected static KCondition ilte(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField1, kValTextField2, "<=");
        
        return kCondition;
    }
    
    protected static KCondition ilk(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn1, kColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilk(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kColumn, kValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilk(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField, kColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilk(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processIBinaryOperator(kValTextField1, kValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilka(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT('%', ").append(", '%')");
        
        kCondition.processIBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilka(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, "%" + newValTextField.params.get(0) + "%");
        } else {
            newValTextField.sb.insert(0, "CONCAT('%', ").append(", '%')");
        }
        
        kCondition.processIBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilka(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT('%', ").append(", '%')");
        
        kCondition.processIBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilka(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, "%" + newValTextField2.params.get(0) + "%");
        } else {
            newValTextField2.sb.insert(0, "CONCAT('%', ").append(", '%')");
        }
        
        kCondition.processIBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilkew(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT('%', ").append("')");
        
        kCondition.processIBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilkew(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, "%" + newValTextField.params.get(0));
        } else {
            newValTextField.sb.insert(0, "CONCAT('%', ").append(")");
        }
        
        kCondition.processIBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilkew(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT('%', ").append(")");
        
        kCondition.processIBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilkew(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, "%" + newValTextField2.params.get(0));
        } else {
            newValTextField2.sb.insert(0, "CONCAT('%', ").append(")");
        }
        
        kCondition.processIBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilksw(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT(").append(", '%')");
        
        kCondition.processIBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilksw(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, newValTextField.params.get(0) + "%");
        } else {
            newValTextField.sb.insert(0, "CONCAT(").append(", '%')");
        }
        
        kCondition.processIBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilksw(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT(").append(", '%')");
        
        kCondition.processIBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition ilksw(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, newValTextField2.params.get(0) + "%");
        } else {
            newValTextField2.sb.insert(0, "CONCAT(").append(", '%')");
        }
        
        kCondition.processIBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition in(
        final KBaseColumn kBaseColumn,
        final Collection values
    ) {
        if (values == null) {
            throw KExceptionHelper.internalServerError("The 'values' param cannot be null");
        }
        
        final KCondition kCondition = new KCondition();
        
        if (values.isEmpty()) {
            kCondition.sb.append("1 = 0");
            
            return kCondition;
        }
        
        kCondition.params.addAll(kBaseColumn.params);
        kCondition.params.addAll(values);
        
        kCondition.sb.append(kBaseColumn.sb).append(" IN (");
        
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                kCondition.sb.append(", ");
            }
            
            kCondition.sb.append("?");
        }
        
        kCondition.sb.append(")");

        return kCondition;
    }
    
    protected static KCondition isFalse(
        final KBaseColumn kBaseColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.params.addAll(kBaseColumn.params);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumn.sb);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" IS FALSE");
        
        return kCondition;
    }
    
    protected static KCondition isNotFalse(
        final KBaseColumn kBaseColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.params.addAll(kBaseColumn.params);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumn.sb);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" IS NOT FALSE");
        
        return kCondition;
    }
    
    protected static KCondition isNotNull(
        final KBaseColumn kBaseColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.params.addAll(kBaseColumn.params);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumn.sb);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" IS NOT NULL");
        
        return kCondition;
    }
    
    protected static KCondition isNull(
        final KBaseColumn kBaseColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.params.addAll(kBaseColumn.params);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumn.sb);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" IS NULL");
        
        return kCondition;
    }
    
    protected static KCondition isNotTrue(
        final KBaseColumn kBaseColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.params.addAll(kBaseColumn.params);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumn.sb);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" IS NOT TRUE");
        
        return kCondition;
    }
    
    protected static KCondition isTrue(
        final KBaseColumn kBaseColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.params.addAll(kBaseColumn.params);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumn.sb);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" IS TRUE");
        
        return kCondition;
    }
    
    protected static KCondition isUnknown(
        final KBaseColumn kBaseColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.params.addAll(kBaseColumn.params);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumn.sb);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" IS UNKNOWN");
        
        return kCondition;
    }
    
    protected static KCondition isNotUnknown(
        final KBaseColumn kBaseColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.params.addAll(kBaseColumn.params);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumn.sb);
        
        if (!kBaseColumn.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" IS NOT UNKNOWN");
        
        return kCondition;
    }
    
    protected static KCondition lk(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processBinaryOperator(kBaseColumn1, kBaseColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lka(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT('%', ").append(", '%')");
        
        kCondition.processBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lka(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, "%" + newValTextField.params.get(0) + "%");
        } else {
            newValTextField.sb.insert(0, "CONCAT('%', ").append(", '%')");
        }
        
        kCondition.processBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lka(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT('%', ").append(", '%')");
        
        kCondition.processBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lka(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, "%" + newValTextField2.params.get(0) + "%");
        } else {
            newValTextField2.sb.insert(0, "CONCAT('%', ").append(", '%')");
        }
        
        kCondition.processBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lkew(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT('%', ").append("')");
        
        kCondition.processBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lkew(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, "%" + newValTextField.params.get(0));
        } else {
            newValTextField.sb.insert(0, "CONCAT('%', ").append(")");
        }
        
        kCondition.processBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lkew(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT('%', ").append(")");
        
        kCondition.processBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lkew(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, "%" + newValTextField2.params.get(0));
        } else {
            newValTextField2.sb.insert(0, "CONCAT('%', ").append(")");
        }
        
        kCondition.processBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lksw(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT(").append(", '%')");
        
        kCondition.processBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lksw(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, newValTextField.params.get(0) + "%");
        } else {
            newValTextField.sb.insert(0, "CONCAT(").append(", '%')");
        }
        
        kCondition.processBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lksw(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT(").append(", '%')");
        
        kCondition.processBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition lksw(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, newValTextField2.params.get(0) + "%");
        } else {
            newValTextField2.sb.insert(0, "CONCAT(").append(", '%')");
        }
        
        kCondition.processBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition neq(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotBinaryOperator(kBaseColumn1, kBaseColumn2, "=");
        
        return kCondition;
    }
    
    protected static KCondition neq(
        final KBaseColumn kBaseColumn1,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processNotBinaryOperator(kBaseColumn1, new KColumn(subQuery.sb, subQuery.params, false), "=");
        
        return kCondition;
    }
    
    protected static KCondition ngt(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotBinaryOperator(kBaseColumn1, kBaseColumn2, ">");
        
        return kCondition;
    }
    
    protected static KCondition ngt(
        final KBaseColumn kBaseColumn1,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processNotBinaryOperator(kBaseColumn1, new KColumn(subQuery.sb, subQuery.params, false), ">");
        
        return kCondition;
    }
    
    protected static KCondition ngte(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotBinaryOperator(kBaseColumn1, kBaseColumn2, ">=");
        
        return kCondition;
    }
    
    protected static KCondition ngte(
        final KBaseColumn kBaseColumn1,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processNotBinaryOperator(kBaseColumn1, new KColumn(subQuery.sb, subQuery.params, false), ">=");
        
        return kCondition;
    }
    
    protected static KCondition nlt(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotBinaryOperator(kBaseColumn1, kBaseColumn2, "<");
        
        return kCondition;
    }
    
    protected static KCondition nlt(
        final KBaseColumn kBaseColumn1,
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processNotBinaryOperator(kBaseColumn1, new KColumn(subQuery.sb, subQuery.params, false), "<");
        
        return kCondition;
    }
    
    protected static KCondition nlte(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotBinaryOperator(kBaseColumn1, kBaseColumn2, "<=");
        
        return kCondition;
    }
    
    protected static KCondition exists(
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processExists(new KColumn(subQuery.sb, subQuery.params, false));
        
        return kCondition;
    }
    
    protected static KCondition nibt(
        final KBaseColumn kBaseColumnValue,
        final KColumn kColumnLow,
        final KColumn kColumnHigh
    ) {
        final KCondition kCondition = new KCondition();
        kCondition.type = CLOSABLE_TYPE;
        
        for (final Object param : kBaseColumnValue.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kColumnLow.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kColumnHigh.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        kCondition.sb.append("NOT (");
        
        if (kBaseColumnValue instanceof KValTextField) {
            if (!kBaseColumnValue.closed) {
                kCondition.sb.append("(");
            }
        } else {
            kCondition.sb.append("LOWER(");
        }
        
        kCondition.sb.append(kBaseColumnValue.sb);
        
        if (kBaseColumnValue instanceof KValTextField) {
            if (!kBaseColumnValue.closed) {
                kCondition.sb.append(")");
            }
        } else {
            kCondition.sb.append(")");
        }
        
        kCondition.sb
            .append(" BETWEEN ")
            .append("LOWER(").append(kColumnLow.sb).append(")")
            .append(" AND ")
            .append("LOWER(").append(kColumnHigh.sb).append(")");
        
        kCondition.sb.append(")");
        
        return kCondition;
    }
    
    protected static KCondition nibt(
        final KBaseColumn kBaseColumnValue,
        final KValTextField kValTextFieldLow,
        final KValTextField kValTextFieldHigh
    ) {
        final KCondition kCondition = new KCondition();
        kCondition.type = CLOSABLE_TYPE;
        
        for (final Object param : kBaseColumnValue.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kValTextFieldLow.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kValTextFieldHigh.params) {
            kCondition.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        kCondition.sb.append("NOT (");
        
        if (kBaseColumnValue instanceof KValTextField) {
            if (!kBaseColumnValue.closed) {
                kCondition.sb.append("(");
            }
        } else {
            kCondition.sb.append("LOWER(");
        }
        
        kCondition.sb.append(kBaseColumnValue.sb);
        
        if (kBaseColumnValue instanceof KValTextField) {
            if (!kBaseColumnValue.closed) {
                kCondition.sb.append(")");
            }
        } else {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" BETWEEN ");
        
        if (!kValTextFieldLow.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kValTextFieldLow.sb);
        
        if (!kValTextFieldLow.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" AND ");
        
        if (!kValTextFieldHigh.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kValTextFieldHigh.sb);
        
        if (!kValTextFieldHigh.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(")");
        
        return kCondition;
    }
    
    protected static KCondition nieq(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn1, kColumn2, "=");
        
        return kCondition;
    }
    
    protected static KCondition nieq(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn, kValTextField, "=");
        
        return kCondition;
    }
    
    protected static KCondition nieq(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField, kColumn, "=");
        
        return kCondition;
    }
    
    protected static KCondition nieq(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField1, kValTextField2, "=");
        
        return kCondition;
    }
    
    protected static KCondition nigt(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn1, kColumn2, ">");
        
        return kCondition;
    }
    
    protected static KCondition nigt(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn, kValTextField, ">");
        
        return kCondition;
    }
    
    protected static KCondition nigt(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField, kColumn, ">");
        
        return kCondition;
    }
    
    protected static KCondition nigt(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField1, kValTextField2, ">");
        
        return kCondition;
    }
    
    protected static KCondition nigte(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn1, kColumn2, ">=");
        
        return kCondition;
    }
    
    protected static KCondition nigte(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn, kValTextField, ">=");
        
        return kCondition;
    }
    
    protected static KCondition nigte(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField, kColumn, ">=");
        
        return kCondition;
    }
    
    protected static KCondition nigte(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField1, kValTextField2, ">=");
        
        return kCondition;
    }
    
    protected static KCondition nilt(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn1, kColumn2, "<");
        
        return kCondition;
    }
    
    protected static KCondition nilt(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn, kValTextField, "<");
        
        return kCondition;
    }
    
    protected static KCondition nilt(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField, kColumn, "<");
        
        return kCondition;
    }
    
    protected static KCondition nilt(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField1, kValTextField2, "<");
        
        return kCondition;
    }
    
    protected static KCondition nilte(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn1, kColumn2, "<=");
        
        return kCondition;
    }
    
    protected static KCondition nilte(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn, kValTextField, "<=");
        
        return kCondition;
    }
    
    protected static KCondition nilte(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField, kColumn, "<=");
        
        return kCondition;
    }
    
    protected static KCondition nilte(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField1, kValTextField2, "<=");
        
        return kCondition;
    }
    
    protected static KCondition nbt(
        final KBaseColumn kBaseColumnValue,
        final KBaseColumn kBaseColumnLow,
        final KBaseColumn kBaseColumnHigh
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.params.addAll(kBaseColumnValue.params);
        kCondition.params.addAll(kBaseColumnLow.params);
        kCondition.params.addAll(kBaseColumnHigh.params);
        
        kCondition.sb.append("NOT (");
        
        if (!kBaseColumnValue.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumnValue.sb);
        
        if (!kBaseColumnValue.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" BETWEEN ");
        
        if (!kBaseColumnLow.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumnLow.sb);
        
        if (!kBaseColumnLow.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(" AND ");
        
        if (!kBaseColumnHigh.closed) {
            kCondition.sb.append("(");
        }
        
        kCondition.sb.append(kBaseColumnHigh.sb);
        
        if (!kBaseColumnHigh.closed) {
            kCondition.sb.append(")");
        }
        
        kCondition.sb.append(")");
        
        return kCondition;
    }
    
    protected static KCondition nilk(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn1, kColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilk(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kColumn, kValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilk(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField, kColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilk(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotIBinaryOperator(kValTextField1, kValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilka(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT('%', ").append(", '%')");
        
        kCondition.processNotIBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilka(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, "%" + newValTextField.params.get(0) + "%");
        } else {
            newValTextField.sb.insert(0, "CONCAT('%', ").append(", '%')");
        }
        
        kCondition.processNotIBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilka(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT('%', ").append(", '%')");
        
        kCondition.processNotIBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilka(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, "%" + newValTextField2.params.get(0) + "%");
        } else {
            newValTextField2.sb.insert(0, "CONCAT('%', ").append(", '%')");
        }
        
        kCondition.processNotIBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilkew(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT('%', ").append("')");
        
        kCondition.processNotIBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilkew(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, "%" + newValTextField.params.get(0));
        } else {
            newValTextField.sb.insert(0, "CONCAT('%', ").append(")");
        }
        
        kCondition.processNotIBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilkew(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT('%', ").append(")");
        
        kCondition.processNotIBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilkew(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, "%" + newValTextField2.params.get(0));
        } else {
            newValTextField2.sb.insert(0, "CONCAT('%', ").append(")");
        }
        
        kCondition.processNotIBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilksw(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT(").append(", '%')");
        
        kCondition.processNotIBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilksw(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, newValTextField.params.get(0) + "%");
        } else {
            newValTextField.sb.insert(0, "CONCAT(").append(", '%')");
        }
        
        kCondition.processNotIBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilksw(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT(").append(", '%')");
        
        kCondition.processNotIBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nilksw(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, newValTextField2.params.get(0) + "%");
        } else {
            newValTextField2.sb.insert(0, "CONCAT(").append(", '%')");
        }
        
        kCondition.processNotIBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlk(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2
    ) {
        final KCondition kCondition = new KCondition();
        
        kCondition.processNotBinaryOperator(kBaseColumn1, kBaseColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlka(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT('%', ").append(", '%')");
        
        kCondition.processNotBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlka(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, "%" + newValTextField.params.get(0) + "%");
        } else {
            newValTextField.sb.insert(0, "CONCAT('%', ").append(", '%')");
        }
        
        kCondition.processNotBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlka(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT('%', ").append(", '%')");
        
        kCondition.processNotBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlka(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, "%" + newValTextField2.params.get(0) + "%");
        } else {
            newValTextField2.sb.insert(0, "CONCAT('%', ").append(", '%')");
        }
        
        kCondition.processNotBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlkew(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT('%', ").append("')");
        
        kCondition.processNotBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlkew(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, "%" + newValTextField.params.get(0));
        } else {
            newValTextField.sb.insert(0, "CONCAT('%', ").append(")");
        }
        
        kCondition.processNotBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlkew(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT('%', ").append(")");
        
        kCondition.processNotBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlkew(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, "%" + newValTextField2.params.get(0));
        } else {
            newValTextField2.sb.insert(0, "CONCAT('%', ").append(")");
        }
        
        kCondition.processNotBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlksw(
        final KColumn kColumn1,
        final KColumn kColumn2
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn2 = kColumn2.cloneMe();
        newKColumn2.sb.insert(0, "CONCAT(").append(", '%')");
        
        kCondition.processNotBinaryOperator(kColumn1, newKColumn2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlksw(
        final KColumn kColumn,
        final KValTextField kValTextField
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField = kValTextField.cloneMe();
        
        if (newValTextField.params.size() == 1) {
            newValTextField.params.set(0, newValTextField.params.get(0) + "%");
        } else {
            newValTextField.sb.insert(0, "CONCAT(").append(", '%')");
        }
        
        kCondition.processNotBinaryOperator(kColumn, newValTextField, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlksw(
        final KValTextField kValTextField,
        final KColumn kColumn
    ) {
        final KCondition kCondition = new KCondition();
        final KColumn newKColumn = kColumn.cloneMe();
        newKColumn.sb.insert(0, "CONCAT(").append(", '%')");
        
        kCondition.processNotBinaryOperator(kValTextField, newKColumn, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition nlksw(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2
    ) {
        final KCondition kCondition = new KCondition();
        final KValTextField newValTextField2 = kValTextField2.cloneMe();
        
        if (newValTextField2.params.size() == 1) {
            newValTextField2.params.set(0, newValTextField2.params.get(0) + "%");
        } else {
            newValTextField2.sb.insert(0, "CONCAT(").append(", '%')");
        }
        
        kCondition.processNotBinaryOperator(kValTextField1, newValTextField2, "LIKE");
        
        return kCondition;
    }
    
    protected static KCondition notExists(
        final KQuery kQuery
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        
        final KCondition kCondition = new KCondition();
        
        final KQueryData subQuery = kQuery.generateSubQueryData();
        
        kCondition.processNotExists(new KColumn(subQuery.sb, subQuery.params, false));
        
        return kCondition;
    }
    
    public static KCondition notIn(
        final KBaseColumn kBaseColumn,
        final Collection values
    ) {
        if (values == null) {
            throw KExceptionHelper.internalServerError("The 'values' param cannot be null");
        }
        
        final KCondition kCondition = new KCondition();
        
        if (values.isEmpty()) {
            kCondition.sb.append("1 = 1");
            
            return kCondition;
        }
        
        kCondition.params.addAll(kBaseColumn.params);
        kCondition.params.addAll(values);
        
        kCondition.sb.append(kBaseColumn.sb).append(" NOT IN (");
        
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                kCondition.sb.append(", ");
            }
            
            kCondition.sb.append("?");
        }
        
        kCondition.sb.append(")");

        return kCondition;
    }
    
    protected String toSql() {
        return sb.toString();
    }
    
    private void processBinaryOperator(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2,
        final String operator
    ) {        
        params.addAll(kBaseColumn1.params);
        params.addAll(kBaseColumn2.params);
        
        if (!kBaseColumn1.closed) {
            this.sb.append("(");
        }
        
        this.sb.append(kBaseColumn1.sb);
        
        if (!kBaseColumn1.closed) {
            this.sb.append(")");
        }
        
        this.sb.append(" ").append(operator).append(" ");
        
        if (!kBaseColumn2.closed) {
            this.sb.append("(");
        }
        
        this.sb.append(kBaseColumn2.sb);
        
        if (!kBaseColumn2.closed) {
            this.sb.append(")");
        }
    }
    
    private void processExists(
        final KBaseColumn kBaseColumn
    ) {
        params.addAll(kBaseColumn.params);
        
        this.sb.append("EXISTS (").append(kBaseColumn.sb).append(")");
    }
    
    private void processNotBinaryOperator(
        final KBaseColumn kBaseColumn1,
        final KBaseColumn kBaseColumn2,
        final String operator
    ) {
        params.addAll(kBaseColumn1.params);
        params.addAll(kBaseColumn2.params);
        
        this.sb.append("NOT (");
        
        if (!kBaseColumn1.closed) {
            this.sb.append("(");
        }
        
        this.sb.append(kBaseColumn1.sb);
        
        if (!kBaseColumn1.closed) {
            this.sb.append(")");
        }
        
        this.sb.append(" ").append(operator).append(" ");
        
        if (!kBaseColumn2.closed) {
            this.sb.append("(");
        }
        
        this.sb.append(kBaseColumn2.sb);
        
        if (!kBaseColumn2.closed) {
            this.sb.append(")");
        }
        
        this.sb.append(")");
    }
    
    private void processNotExists(
        final KBaseColumn kBaseColumn
    ) {
        params.addAll(kBaseColumn.params);
        
        this.sb.append("NOT EXISTS (").append(kBaseColumn.sb).append(")");
    }
    
    private void processNotIBinaryOperator(
        final KColumn kColumn1,
        final KColumn kColumn2,
        final String operator
    ) {
        for (final Object param : kColumn1.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }

        for (final Object param : kColumn2.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        this.sb.append("NOT (LOWER(").append(kColumn1.sb).append(") ").append(operator).append(" LOWER(").append(kColumn2.sb).append("))");
    }
    
    private void processNotIBinaryOperator(
        final KColumn kColumn,
        final KValTextField kValTextField,
        final String operator
    ) {
        for (final Object param : kColumn.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kValTextField.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        this.sb.append("NOT (LOWER(").append(kColumn.sb).append(") ").append(operator).append(" ").append(kValTextField.sb).append(")");
    }
    
    private void processNotIBinaryOperator(
        final KValTextField kValTextField,
        final KBaseColumn kBaseColumn,
        final String operator
    ) {
        for (final Object param : kValTextField.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kBaseColumn.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        this.sb.append("NOT (").append(kValTextField.sb).append(" ").append(operator).append(" LOWER(").append(kBaseColumn.sb).append("))");
    }
    
    private void processNotIBinaryOperator(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2,
        final String operator
    ) {
        for (final Object param : kValTextField1.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }

        for (final Object param : kValTextField2.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        this.sb.append("NOT (").append(kValTextField1.sb).append(" ").append(operator).append(" ").append(kValTextField2.sb).append(")");
    }
    
    private void processIBinaryOperator(
        final KColumn kColumn1,
        final KColumn kColumn2,
        final String operator
    ) {
        for (final Object param : kColumn1.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }

        for (final Object param : kColumn2.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        this.sb.append("LOWER(").append(kColumn1.sb).append(") ").append(operator).append(" LOWER(").append(kColumn2.sb).append(")");
    }
    
    private void processIBinaryOperator(
        final KColumn kColumn,
        final KValTextField kValTextField,
        final String operator
    ) {
        for (final Object param : kColumn.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kValTextField.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        this.sb.append("LOWER(").append(kColumn.sb).append(") ").append(operator).append(" ").append(kValTextField.sb);
    }
    
    private void processIBinaryOperator(
        final KValTextField kValTextField,
        final KColumn kColumn,
        final String operator
    ) {
        for (final Object param : kValTextField.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        for (final Object param : kColumn.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        this.sb.append(kValTextField.sb).append(" ").append(operator).append(" LOWER(").append(kColumn.sb).append(")");
    }
    
    private void processIBinaryOperator(
        final KValTextField kValTextField1,
        final KValTextField kValTextField2,
        final String operator
    ) {
        for (final Object param : kValTextField1.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }

        for (final Object param : kValTextField2.params) {
            this.params.add(param instanceof String ? param.toString().toLowerCase() : param);
        }
        
        this.sb.append(kValTextField1.sb).append(" ").append(operator).append(" ").append(kValTextField2.sb);
    }
}
