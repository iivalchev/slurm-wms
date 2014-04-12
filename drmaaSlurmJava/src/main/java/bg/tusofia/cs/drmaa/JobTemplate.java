package bg.tusofia.cs.drmaa;

import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_BLOCK_EMAIL;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_SUCCESS;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERROR_PATH;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_INPUT_PATH;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_JOB_CATEGORY;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_JOB_NAME;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_JOIN_FILES;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_JS_STATE;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_NATIVE_SPECIFICATION;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_OUTPUT_PATH;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_REMOTE_COMMAND;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_START_TIME;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_SUBMISSION_STATE_ACTIVE;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_SUBMISSION_STATE_HOLD;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_V_ARGV;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_V_EMAIL;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_V_ENV;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_WD;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_set_attribute;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_set_vector_attribute;
import static org.bridj.Pointer.pointerToCString;
import static org.bridj.Pointer.pointerToCStrings;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bridj.Pointer;
import org.ggf.drmaa.ConflictingAttributeValuesException;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.InvalidAttributeValueException;
import org.ggf.drmaa.PartialTimestamp;
import org.ggf.drmaa.SimpleJobTemplate;

import bg.tusofia.cs.drmaa.DrmaaC.drmaa_job_template_s;

public class JobTemplate extends SimpleJobTemplate {

	private static final long serialVersionUID = 1L;

	private static final String DATE_FORMAT = "YYYY/MM/DD hh:mm:ss";

	private final Pointer<Pointer<DrmaaC.drmaa_job_template_s>> jtPointer;

	JobTemplate(Pointer<Pointer<drmaa_job_template_s>> jtPointer) {
		this.jtPointer = jtPointer;
	}

	Pointer<Pointer<DrmaaC.drmaa_job_template_s>> getJtPointer() {
		return jtPointer;
	}

	@Override
	public void setRemoteCommand(String remoteCommand) throws DrmaaException {
		super.setRemoteCommand(remoteCommand);
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_REMOTE_COMMAND),
				pointerToCString(remoteCommand),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	@Override
	public void setArgs(List args) throws DrmaaException {
		super.setArgs(args);
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_vector_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_V_ARGV),
				pointerToCStrings((String[]) args.toArray()),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setJobSubmissionState(int state) throws DrmaaException {
		super.setJobSubmissionState(state);
		Pointer<Byte> jsState = pointerToCString(DRMAA_SUBMISSION_STATE_ACTIVE);
		if (state == HOLD_STATE) {
			jsState = pointerToCString(DRMAA_SUBMISSION_STATE_HOLD);
		}
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_JS_STATE),
				jsState,
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setJobEnvironment(Map env) throws DrmaaException {
		super.setJobEnvironment(env);
		String[] envArr = new String[env.size()];
		Set<Map.Entry> entrySet = env.entrySet();
		int i = 0;
		for (Map.Entry e : entrySet) {
			// TODO: move the format to .properties file
			envArr[i++] = e.getKey().toString() + "=" + e.getValue().toString();
		}
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_vector_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_V_ENV),
				pointerToCStrings(envArr),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setWorkingDirectory(String wd) throws DrmaaException {
		super.setWorkingDirectory(wd);
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_WD),
				pointerToCString(wd),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setJobCategory(String category) throws DrmaaException {
		super.setJobCategory(category);
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_JOB_CATEGORY),
				pointerToCString(category),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setNativeSpecification(String spec) throws DrmaaException {
		super.setNativeSpecification(spec);
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_NATIVE_SPECIFICATION),
				pointerToCString(spec),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setEmail(Set email) throws DrmaaException {
		super.setEmail(email);
		String[] emailAttr = (String[]) email.toArray();
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_vector_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_V_EMAIL),
				pointerToCStrings(emailAttr),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setBlockEmail(boolean blockEmail) throws DrmaaException {
		super.setBlockEmail(blockEmail);
		String blockEmailAttr = blockEmail ? "1" : "0";
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_BLOCK_EMAIL),
				pointerToCString(blockEmailAttr),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setStartTime(PartialTimestamp startTime) throws DrmaaException {
		super.setStartTime(startTime);
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_START_TIME),
				pointerToCString(new SimpleDateFormat(DATE_FORMAT)
						.format(startTime.getTime())),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setJobName(String name) throws DrmaaException {
		super.setJobName(name);
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_JOB_NAME),
				pointerToCString(name),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setInputPath(String inputPath) throws DrmaaException {
		super.setInputPath(inputPath);
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_INPUT_PATH),
				pointerToCString(this.inputPath),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setOutputPath(String outputPath) throws DrmaaException {
		super.setOutputPath(outputPath);
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_OUTPUT_PATH),
				pointerToCString(outputPath),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setErrorPath(String errorPath) throws DrmaaException {
		super.setErrorPath(errorPath);
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_ERROR_PATH),
				pointerToCString(errorPath),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	public void setJoinFiles(boolean join) throws DrmaaException {
		super.setJoinFiles(join);
		String joinFilesAttr = join ? "y" : "n";
		Buffer eb = Buffer.newErrorBuffer();
		int result = drmaa_set_attribute(
				jtPointer.get(),
				pointerToCString(DRMAA_JOIN_FILES),
				pointerToCString(joinFilesAttr),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_ATTRIBUTE_VALUE:
				e = new InvalidAttributeValueException(errorMessage);
				break;
			case DRMAA_ERRNO_CONFLICTING_ATTRIBUTE_VALUES:
				e = new ConflictingAttributeValuesException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}
}
