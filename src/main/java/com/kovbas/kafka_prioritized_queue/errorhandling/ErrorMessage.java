package com.kovbas.kafka_prioritized_queue.errorhandling;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ErrorMessage {

    /** Contains the same HTTP Status code returned by the server */
    private Integer status;

    /** Application specific error code */
    private Integer code;

    /** Message describing the error*/
    private String message;

    /** Extra information that might useful for developers */
    private String developerMessage;

    /**
     * Creates the ErrorMessage from the NotFoundException object
     * @param e Exception
     */
    public ErrorMessage(Throwable e, Integer status){

        setStatus(status);
        setCode(status);
        setMessage(e.getMessage());

        StringWriter errorStackTrace = new StringWriter();
        e.printStackTrace(new PrintWriter(errorStackTrace));
        setDeveloperMessage(errorStackTrace.toString());
    }

    /**
     * Create ErrorMessage object without populating int properties
     */
    public ErrorMessage() {}

    /**
     * @return Returns error status
     */
    public Integer getStatus() {
        return status;
    }

    /**
     * Sets status
     * @param status Error status
     */
    public void setStatus(Integer status) {
        this.status = status;
    }

    /**
     * @return Returns error code
     */
    public Integer getCode() {
        return code;
    }

    /**
     * Sets code
     * @param code Error code
     */
    public void setCode(Integer code) {
        this.code = code;
    }

    /**
     * @return Returns error message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Sets message
     * @param message Error message
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * @return Returns developer message
     */
    public String getDeveloperMessage() {
        return developerMessage;
    }

    /**
     * Set developer message
     * @param developerMessage Developer message
     */
    public void setDeveloperMessage(String developerMessage) {
        this.developerMessage = developerMessage;
    }
}