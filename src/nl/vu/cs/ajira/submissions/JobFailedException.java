package nl.vu.cs.ajira.submissions;

/**
 * This exception gets thrown when a job fails for some reason.
 * The cause of the failure is specified as a nested throwable.
 */
public class JobFailedException extends Exception {
       
    /**
     * Constructs a new JobFailedException with the specified detail message and cause.
     * @param description the detail message
     * @param cause the cause
     */
    public JobFailedException(String description, Throwable cause) {
	super(description, cause);
    }
    
    /**
     * Constructs a new JobFailedException with the specified cause and a detail message
     * obtained from the specified cause.
     * @param cause the cause
     */
    public JobFailedException(Throwable cause) {
	super(cause);
    }
}
