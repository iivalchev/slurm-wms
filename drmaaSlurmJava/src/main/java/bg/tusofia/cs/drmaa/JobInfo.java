package bg.tusofia.cs.drmaa;

import static bg.tusofia.cs.drmaa.Buffer.newErrorBuffer;
import static bg.tusofia.cs.drmaa.Buffer.newRusageBuffer;
import static bg.tusofia.cs.drmaa.Buffer.newTermSignalBuffer;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_NO_MORE_ELEMENTS;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_SUCCESS;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_get_next_attr_value;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_wcoredump;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_wexitstatus;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_wifaborted;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_wifexited;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_wifsignaled;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_wtermsig;
import static org.bridj.Pointer.allocateInt;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bridj.Pointer;
import org.ggf.drmaa.DrmaaException;

import bg.tusofia.cs.drmaa.DrmaaC.drmaa_attr_values_s;

public class JobInfo implements org.ggf.drmaa.JobInfo {

	private static final String RUSAGE_PATTERN_REGEX = "(.+)=(.+)";
	private static final Pattern RUSAGE_PATTERN = Pattern
			.compile(RUSAGE_PATTERN_REGEX);

	private final String jobId;
	private final Pointer<Integer> stat;
	private final Pointer<drmaa_attr_values_s> rusage;

	JobInfo(String jobId, Pointer<Integer> stat,
			Pointer<drmaa_attr_values_s> rusage) {
		this.jobId = jobId;
		this.stat = stat;
		this.rusage = rusage;
	}

	@Override
	public String getJobId() throws DrmaaException {
		return jobId;
	}

	@Override
	public Map getResourceUsage() throws DrmaaException {
		Map<String, String> resourceUsageMap = new HashMap<String, String>();
		int result;
		do {
			Buffer rb = newRusageBuffer();
			result = drmaa_get_next_attr_value(
					rusage,
					rb.getPointer(),
					rb.getLength());
			Matcher m = RUSAGE_PATTERN.matcher(rb.toString());
			if (m.find()) {
				String key = m.group(1);
				String value = m.group(2);
				resourceUsageMap.put(key, value);
			}
		} while (result != DRMAA_ERRNO_NO_MORE_ELEMENTS);
		return resourceUsageMap;
	}

	@Override
	public boolean hasExited() throws DrmaaException {
		Pointer<Integer> exited = allocateInt();
		Buffer eb = newErrorBuffer();
		int result = drmaa_wifexited(
				exited,
				stat.get(),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			throw new PlainDrmaaException(eb.toString());
		}
		return exited.get() != 0;
	}

	@Override
	public int getExitStatus() throws DrmaaException {
		if (!hasExited()) {
			throw new IllegalStateException("Job with Job Id = " + jobId
					+ " has hasExited() == false");
		}
		Pointer<Integer> exitStatus = allocateInt();
		Buffer eb = newErrorBuffer();
		int result = drmaa_wexitstatus(
				exitStatus,
				stat.get(),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			throw new PlainDrmaaException(eb.toString());
		}
		return exitStatus.get();
	}

	@Override
	public boolean hasSignaled() throws DrmaaException {
		Pointer<Integer> signaled = allocateInt();
		Buffer eb = newErrorBuffer();
		int result = drmaa_wifsignaled(
				signaled,
				stat.get(),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			throw new PlainDrmaaException(eb.toString());
		}
		return signaled.get() != 0;
	}

	@Override
	public String getTerminatingSignal() throws DrmaaException {
		if (!hasSignaled()) {
			throw new IllegalStateException("Job with Job Id = " + jobId
					+ " has hasSignaled() == false");
		}
		Buffer termSignal = newTermSignalBuffer();
		Buffer eb = newErrorBuffer();
		int result = drmaa_wtermsig(
				termSignal.getPointer(),
				termSignal.getLength(),
				stat.get(),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			throw new PlainDrmaaException(eb.toString());
		}
		return termSignal.toString();
	}

	@Override
	public boolean hasCoreDump() throws DrmaaException {
		if (!hasSignaled()) {
			throw new IllegalStateException("Job with Job Id = " + jobId
					+ " has hasSignaled() == false");
		}
		Pointer<Integer> hasCoreDump = allocateInt();
		Buffer eb = newErrorBuffer();
		int result = drmaa_wcoredump(
				hasCoreDump,
				stat.get(),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			throw new PlainDrmaaException(eb.toString());
		}
		return hasCoreDump.get() != 0;
	}

	@Override
	public boolean wasAborted() throws DrmaaException {
		Pointer<Integer> aborted = allocateInt();
		Buffer eb = newErrorBuffer();
		int result = drmaa_wifaborted(
				aborted,
				stat.get(),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			throw new PlainDrmaaException(eb.toString());
		}
		return aborted.get() != 0;
	}
}
