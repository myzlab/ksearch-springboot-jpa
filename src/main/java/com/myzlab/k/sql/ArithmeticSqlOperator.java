package com.myzlab.k.sql;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ArithmeticSqlOperator {
    
    public static final ArithmeticSqlOperator ADD = new ArithmeticSqlOperator("+");
    public static final ArithmeticSqlOperator SUBTRACT = new ArithmeticSqlOperator("-");
    public static final ArithmeticSqlOperator MULTIPLY = new ArithmeticSqlOperator("*");
    public static final ArithmeticSqlOperator DIVIDE = new ArithmeticSqlOperator("/");
    public static final ArithmeticSqlOperator MODULO = new ArithmeticSqlOperator("%");
    
    private final String sql;
}