package com.kovbas.kafka_prioritized_queue.http.errorhandling;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.NoHandlerFoundException;

@ControllerAdvice
@RestController
public class GlobalExceptionHandler {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(value = Throwable.class)
    public ErrorMessage handleGeneralException(Throwable e){

        log.error(e.getMessage());
        e.printStackTrace();

        return new ErrorMessage(e, HttpStatus.INTERNAL_SERVER_ERROR.value());
    }

    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(value = NoHandlerFoundException.class)
    public ErrorMessage handleNoHandlerFoundException(NoHandlerFoundException e){

        log.error(e.getMessage());
        e.printStackTrace();

        return new ErrorMessage(e, HttpStatus.NOT_FOUND.value());
    }
}
