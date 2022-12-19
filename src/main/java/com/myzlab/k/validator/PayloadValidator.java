package com.myzlab.k.validator;

import com.myzlab.k.helper.KExceptionHelper;
import java.util.ArrayList;
import java.util.Set;
import javax.validation.Configuration;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

public class PayloadValidator {
    
    private static final Validator validator;

    static {
        final Configuration<?> config = Validation.byDefaultProvider().configure();
        final ValidatorFactory factory = config.buildValidatorFactory();
        
        validator = factory.getValidator();
        
        factory.close();
    }

    public void validate() {
        final Set<ConstraintViolation<PayloadValidator>> constraintViolations = validator.validate(this);
        
        if (!constraintViolations.isEmpty()) {
            throw KExceptionHelper.badRequest(new ArrayList<>(constraintViolations).get(0).getMessage());
        }
        
    }
}
