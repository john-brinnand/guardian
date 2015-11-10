package spongecell.guardian.agent.exception;

public class GuardianWorkFlowException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	
	public GuardianWorkFlowException() {
		super();
	}
	
	public GuardianWorkFlowException (String message) {
		super(message);
	}
	
	public GuardianWorkFlowException(Throwable cause) {
		super(cause);
	}
	
	public GuardianWorkFlowException (String message, Throwable cause) {
		super(message, cause);
	}	

}
