package com.myzlab.k.helper;

import com.myzlab.k.KException;
import org.springframework.http.HttpStatus;

public class KExceptionHelper {
    
    public static KException notFound() {
        return new KException(HttpStatus.NOT_FOUND, "Resource not found");
    }
    
    public static KException notFound(final String message) {
        return new KException(HttpStatus.NOT_FOUND, message);
    }
    
    public static KException badRequest(final String message) {
        return new KException(HttpStatus.BAD_REQUEST, message);
    }
    
    public static KException internalServerError() {
        return new KException(HttpStatus.INTERNAL_SERVER_ERROR);
    }
    
    public static KException internalServerError(final String message) {
        return new KException(HttpStatus.INTERNAL_SERVER_ERROR, message);
    }
    
    public static KException unauthorized(final String message) {
        return new KException(HttpStatus.UNAUTHORIZED, message);
    }
    
    public static KException forbidden(final String message) {
        return new KException(HttpStatus.FORBIDDEN, message);
    }
    
    public static KException iAmATeapot(final String message) {
        return new KException(HttpStatus.I_AM_A_TEAPOT, message);
    }
    
    public static KException createByStatusCode(final int statusCode, final String message) {
        switch (statusCode) {
            case 200:
                return new KException(HttpStatus.OK, message);
            case 201:
                return new KException(HttpStatus.CREATED, message);
            case 202:
                return new KException(HttpStatus.ACCEPTED, message);
            case 204:
                return new KException(HttpStatus.NO_CONTENT, message);
            case 301:
                return new KException(HttpStatus.MOVED_PERMANENTLY, message);
            case 302:
                return new KException(HttpStatus.FOUND, message);
            case 400:
                return new KException(HttpStatus.BAD_REQUEST, message);
            case 401:
                return new KException(HttpStatus.UNAUTHORIZED, message);
            case 402:
                return new KException(HttpStatus.PAYMENT_REQUIRED, message);
            case 403:
                return new KException(HttpStatus.FORBIDDEN, message);
            case 404:
                return new KException(HttpStatus.NOT_FOUND, message);
            case 405:
                return new KException(HttpStatus.METHOD_NOT_ALLOWED, message);
            case 418:
                return new KException(HttpStatus.I_AM_A_TEAPOT, message);
            case 429:
                return new KException(HttpStatus.TOO_MANY_REQUESTS, message);
            case 500:
                return new KException(HttpStatus.INTERNAL_SERVER_ERROR, message);
            case 502:
                return new KException(HttpStatus.BAD_GATEWAY, message);
            default:
                return new KException(HttpStatus.INTERNAL_SERVER_ERROR, message);
        }
    }
}
