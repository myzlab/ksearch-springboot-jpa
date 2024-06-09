package com.myzlab.k.sql;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ArithmeticSqlBinaryOperator {
    
    public static final ArithmeticSqlBinaryOperator ADD = new ArithmeticSqlBinaryOperator("+");
    public static final ArithmeticSqlBinaryOperator SUBTRACT = new ArithmeticSqlBinaryOperator("-");
    public static final ArithmeticSqlBinaryOperator MULTIPLY = new ArithmeticSqlBinaryOperator("*");
    public static final ArithmeticSqlBinaryOperator DIVIDE = new ArithmeticSqlBinaryOperator("/");
    public static final ArithmeticSqlBinaryOperator MODULO = new ArithmeticSqlBinaryOperator("%");
    public static final ArithmeticSqlBinaryOperator EXPONENTIATION = new ArithmeticSqlBinaryOperator("^");
    public static final ArithmeticSqlBinaryOperator BITWISE_AND = new ArithmeticSqlBinaryOperator("&");
    public static final ArithmeticSqlBinaryOperator BITWISE_OR = new ArithmeticSqlBinaryOperator("|");
    public static final ArithmeticSqlBinaryOperator BITWISE_XOR = new ArithmeticSqlBinaryOperator("#");
    public static final ArithmeticSqlBinaryOperator BITWISE_SHIFT_LEFT = new ArithmeticSqlBinaryOperator("<<");
    public static final ArithmeticSqlBinaryOperator BITWISE_SHIFT_RIGHT = new ArithmeticSqlBinaryOperator(">>");
    
    private final String sql;
}