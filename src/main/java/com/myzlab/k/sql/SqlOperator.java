package com.myzlab.k.sql;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum SqlOperator {
    ADD("+"), 
    SUBTRACT("-"), 
    MULTIPLY("*"), 
    DIVIDE("/"),
    MODULO("%"),
//    CONCAT("||"),
//    BIT_AND("&"),
//    BIT_OR("|"),
//    BIT_XOR("#"),
//    LEFT_SHIFT("<<"),
//    RIGHT_SHIFT(">>"),
//    OVERLAPS("&&"),
//    CONTAINS("@>"),
    GIST_INDEX_SEARCH("%"),
    TRIGRAM_SEARCH("%");
//    JSON_GET_VALUE("->"),
//    JSON_GET_PATH_VALUE("->>"),
//    JSON_CONTAINS("@>"),
//    JSON_CONTAINS_ANY_KEY("?"),
//    JSON_CONTAINS_ALL_KEYS("?&"),
//    JSON_CONTAINS_ANY_KEYS("?|"),
//    TEXT_SEARCH_MATCH("@@");

    private final String sql;
}