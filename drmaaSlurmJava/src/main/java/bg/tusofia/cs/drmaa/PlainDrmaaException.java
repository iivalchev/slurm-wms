package bg.tusofia.cs.drmaa;

import org.ggf.drmaa.DrmaaException;

public class PlainDrmaaException extends DrmaaException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public PlainDrmaaException() {
	}

	public PlainDrmaaException(String msg) {
		super(msg);
	}
}
