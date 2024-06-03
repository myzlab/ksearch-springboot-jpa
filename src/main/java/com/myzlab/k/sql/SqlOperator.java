package com.myzlab.k.sql;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum SqlOperator {
    // Arithmetic operators
    ADD("+"), 
    SUBTRACT("-"), 
    MULTIPLY("*"), 
    DIVIDE("/"),
    MODULO("%"),

    // Concatenation operator
//    CONCAT("||"),

    // Bitwise operators
//    BIT_AND("&"),
//    BIT_OR("|"),
//    BIT_XOR("#"),
//    LEFT_SHIFT("<<"),
//    RIGHT_SHIFT(">>"),

    // Overlap operator
//    OVERLAPS("&&"),

    // Containment operators
//    CONTAINS("@>"),

    // JSON operators
//    JSON_GET_VALUE("->"),
//    JSON_GET_PATH_VALUE("->>"),
//    JSON_CONTAINS("@>"),
//    JSON_CONTAINS_ANY_KEY("?"),
//    JSON_CONTAINS_ALL_KEYS("?&"),
//    JSON_CONTAINS_ANY_KEYS("?|"),

    // Full-text search operator
//    TEXT_SEARCH_MATCH("@@"),

    // TRGM operators
    TRGM_SIMILARITY("%"),
    TRGM_WORD_SIMILARITY("<%"),
    TRGM_WORD_SIMILARITY_COMMUTATOR("%>"),
    TRGM_STRICT_WORD_SIMILARITY("<<%"),
    TRGM_STRICT_WORD_SIMILARITY_COMMUTATOR("%>>"),
    TRGM_SIMILARITY_DISTANCE("<->"),
    TRGM_WORD_SIMILARITY_DISTANCE("<<->"),
    TRGM_WORD_SIMILARITY_DISTANCE_COMMUTATOR("<->>"),
    TRGM_STRICT_WORD_SIMILARITY_DISTANCE("<<<->"),
    TRGM_STRICT_WORD_SIMILARITY_DISTANCE_COMMUTATOR("<->>>");


    private final String sql;
}