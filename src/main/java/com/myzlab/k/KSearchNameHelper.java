package com.myzlab.k;

import org.apache.commons.lang3.StringUtils;

public class KSearchNameHelper {
//    public static String generateTableAlias(final String name) {
//        if (name == null || name.isEmpty()) {
//            return "";
//        }
//        
//        if (!name.contains("_")) {
//            if (name.length() > 2) {
//                return name.substring(0, 2);
//            }
//            
//            return name;
//        }
//        
//        final StringBuilder stringBuilder = new StringBuilder();
//        final String[] parts = name.split("_");
//        
//        for (final String part : parts) {
//            if (part != null && !part.isEmpty()) {
//                stringBuilder.append(part.charAt(0));
//            }
//        }
//        
//        return stringBuilder.toString();
//    }
    
//    public static String generateAttributeName(final String name) {
//        if (name == null || name.isEmpty()) {
//            return "";
//        }
//        
//        if (!name.contains("_")) {
//            return name;
//        }
//        
//        final StringBuilder stringBuilder = new StringBuilder();
//        final String[] parts = name.split("_");
//        
//        int i = 0;
//        
//        for (final String part : parts) {
//            if (part != null && !part.isEmpty()) {
//                if (i > 0) {
//                    stringBuilder.append(KSearchHelper.capitalize(part));
//                } else {
//                    stringBuilder.append(part);
//                }
//                
//                i++;
//            }
//        }
//        
//        return stringBuilder.toString();
//    }
//    
//    public static String generateClassName(final String name) {
//        if (name == null || name.isEmpty()) {
//            return "";
//        }
//        
//        if (!name.contains("_")) {
//            return KSearchHelper.capitalize(name);
//        }
//        
//        final StringBuilder stringBuilder = new StringBuilder();
//        final String[] parts = name.split("_");
//        
//        for (final String part : parts) {
//            if (part != null && !part.isEmpty()) {
//                stringBuilder.append(KSearchHelper.capitalize(part));
//            }
//        }
//        
//        return stringBuilder.toString();
//    }
//    
    public static String capitalize(final String name) {
        return StringUtils.capitalize(name);
    }
    
    protected static String generateGetName(final String name) {
        if (name == null || name.isEmpty()) {
            return "";
        }
        
        if (!name.contains("_")) {
            return "get" + KSearchNameHelper.capitalize(name);
        }

        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("get");
        
        final String[] parts = name.split("_");
        
        for (final String part : parts) {
            if (part != null && !part.isEmpty()) {
                stringBuilder.append(KSearchNameHelper.capitalize(part));
            }
        }
        
        return stringBuilder.toString();
    }
    
    protected static String generateSetName(final String name) {
        if (name == null || name.isEmpty()) {
            return "";
        }
        
        if (!name.contains("_")) {
            return "set" + KSearchNameHelper.capitalize(name);
        }

        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("set");
        
        final String[] parts = name.split("_");
        
        for (final String part : parts) {
            if (part != null && !part.isEmpty()) {
                stringBuilder.append(KSearchNameHelper.capitalize(part));
            }
        }
        
        return stringBuilder.toString();
    }
    
//    public static String generateAttributeStaticName(final String name) {
//        if (name == null || name.isEmpty()) {
//            return "";
//        }
//        
//        return name.toUpperCase();
//    }
}
