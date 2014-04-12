package bg.tusofia.cs.drmaa;

import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERROR_STRING_BUFFER;
import static org.bridj.Pointer.allocateBytes;

import org.bridj.Pointer;

public class Buffer {

	private final long length;
	private final Pointer<Byte> pointer;

	public static Buffer newErrorBuffer() {
		return new Buffer(DRMAA_ERROR_STRING_BUFFER);
	}

	public static Buffer newJobIdBuffer() {
		// Not sure what the JOB_ID buffer length should be.
		// Stick with DRMAA_ERROR_STRING_BUFFER.
		// It should big enough.
		return new Buffer(DRMAA_ERROR_STRING_BUFFER);
	}

	public static Buffer newTermSignalBuffer() {
		// Not sure what the buffer length should be.
		// Stick with DRMAA_ERROR_STRING_BUFFER.
		// It should big enough.
		return new Buffer(DRMAA_ERROR_STRING_BUFFER);
	}

	public static Buffer newRusageBuffer() {
		// Not sure what the buffer length should be.
		// Stick with DRMAA_ERROR_STRING_BUFFER.
		// It should big enough.
		return new Buffer(DRMAA_ERROR_STRING_BUFFER);
	}

	public static Buffer newContactBuffer() {
		// Not sure what the buffer length should be.
		// Stick with DRMAA_ERROR_STRING_BUFFER.
		// It should big enough.
		return new Buffer(DRMAA_ERROR_STRING_BUFFER);
	}

	public static Buffer newDrmSystemBuffer() {
		// Not sure what the buffer length should be.
		// Stick with DRMAA_ERROR_STRING_BUFFER.
		// It should big enough.
		return new Buffer(DRMAA_ERROR_STRING_BUFFER);
	}

	public static Buffer newDrmImplementationBuffer() {
		// Not sure what the buffer length should be.
		// Stick with DRMAA_ERROR_STRING_BUFFER.
		// It should big enough.
		return new Buffer(DRMAA_ERROR_STRING_BUFFER);
	}

	public Buffer(long length) {
		this.length = length;
		this.pointer = allocateBytes(length);
	}

	public long getLength() {
		return length;
	}

	public Pointer<Byte> getPointer() {
		return pointer;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Byte b : pointer) {
			if (b == 0x00) {
				break;
			}
			sb.append((char) (b & 0xFF));
		}
		return sb.toString();
	}
}
