package spongecell.guardian.agent.yarn.exception;

public class YarnResourceManagerException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	
	public YarnResourceManagerException() {
		super();
	}
	
	public YarnResourceManagerException (String message) {
		super(message);
	}
	
	public YarnResourceManagerException(Throwable cause) {
		super(cause);
	}
	
	public YarnResourceManagerException (String message, Throwable cause) {
		super(message, cause);
	}	

}
