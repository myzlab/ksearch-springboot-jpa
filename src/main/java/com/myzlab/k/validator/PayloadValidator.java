package com.myzlab.k.validator;

import com.myzlab.k.helper.KExceptionHelper;
import java.util.ArrayList;
import java.util.Set;
import jakarta.validation.Configuration;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;

public class PayloadValidator {
    
    private static final Validator validator;

    static {
        Configuration<?> config = Validation.byDefaultProvider().configure();
        ValidatorFactory factory = config.buildValidatorFactory();
        
        validator = factory.getValidator();
        
        factory.close();
    }

    public void validate() {
        Set<ConstraintViolation<PayloadValidator>> constraintViolations = validator.validate(this);
        
        if (!constraintViolations.isEmpty()) {
            throw KExceptionHelper.badRequest(new ArrayList<>(constraintViolations).get(0).getMessage());
        }
    }
}
