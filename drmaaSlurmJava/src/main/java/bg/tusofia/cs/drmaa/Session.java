package bg.tusofia.cs.drmaa;

import static bg.tusofia.cs.drmaa.Buffer.newErrorBuffer;
import static bg.tusofia.cs.drmaa.Buffer.newJobIdBuffer;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_ALREADY_ACTIVE_SESSION;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_DEFAULT_CONTACT_STRING_ERROR;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_DENIED_BY_DRM;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_DRMS_EXIT_ERROR;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_EXIT_TIMEOUT;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_HOLD_INCONSISTENT_STATE;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_INVALID_CONTACT_STRING;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_INVALID_JOB;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_NO_ACTIVE_SESSION;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_NO_DEFAULT_CONTACT_STRING_SELECTED;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_NO_MORE_ELEMENTS;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_RELEASE_INCONSISTENT_STATE;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_RESUME_INCONSISTENT_STATE;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_SUCCESS;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_SUSPEND_INCONSISTENT_STATE;
import static bg.tusofia.cs.drmaa.DrmaaC.DRMAA_ERRNO_TRY_LATER;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_allocate_job_template;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_control;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_delete_job_template;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_exit;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_get_DRMAA_implementation;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_get_DRM_system;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_get_contact;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_get_next_job_id;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_get_num_job_ids;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_init;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_job_ps;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_release_job_ids;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_run_bulk_jobs;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_run_job;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_synchronize;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_version;
import static bg.tusofia.cs.drmaa.DrmaaC.drmaa_wait;
import static org.bridj.Pointer.allocateInt;
import static org.bridj.Pointer.allocatePointer;
import static org.bridj.Pointer.allocateSizeT;
import static org.bridj.Pointer.pointerToCString;
import static org.bridj.Pointer.pointerToCStrings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bridj.Pointer;
import org.bridj.SizeT;
import org.ggf.drmaa.AlreadyActiveSessionException;
import org.ggf.drmaa.DefaultContactStringException;
import org.ggf.drmaa.DeniedByDrmException;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.DrmsExitException;
import org.ggf.drmaa.ExitTimeoutException;
import org.ggf.drmaa.HoldInconsistentStateException;
import org.ggf.drmaa.InvalidContactStringException;
import org.ggf.drmaa.InvalidJobException;
import org.ggf.drmaa.InvalidJobTemplateException;
import org.ggf.drmaa.NoActiveSessionException;
import org.ggf.drmaa.NoDefaultContactStringException;
import org.ggf.drmaa.ReleaseInconsistentStateException;
import org.ggf.drmaa.ResumeInconsistentStateException;
import org.ggf.drmaa.SuspendInconsistentStateException;
import org.ggf.drmaa.TryLaterException;
import org.ggf.drmaa.Version;

import bg.tusofia.cs.drmaa.DrmaaC.drmaa_attr_values_s;

/**
 * 
 * @author Ivan Valchev (ivalchev,work@gmail.com)
 * 
 */
public class Session implements org.ggf.drmaa.Session {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init(String contact) throws DrmaaException {
		Buffer eb = newErrorBuffer();
		int result = drmaa_init(
				pointerToCString(contact),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_INVALID_CONTACT_STRING:
				e = new InvalidContactStringException(errorMessage);
				break;
			case DRMAA_ERRNO_ALREADY_ACTIVE_SESSION:
				e = new AlreadyActiveSessionException(errorMessage);
				break;
			case DRMAA_ERRNO_NO_DEFAULT_CONTACT_STRING_SELECTED:
				e = new NoDefaultContactStringException(errorMessage);
				break;
			case DRMAA_ERRNO_DEFAULT_CONTACT_STRING_ERROR:
				e = new DefaultContactStringException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void exit() throws DrmaaException {
		Buffer eb = newErrorBuffer();
		int result = drmaa_exit(eb.getPointer(), eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_DRMS_EXIT_ERROR:
				e = new DrmsExitException(errorMessage);
				break;
			case DRMAA_ERRNO_NO_ACTIVE_SESSION:
				e = new NoActiveSessionException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobTemplate createJobTemplate() throws DrmaaException {
		Buffer eb = newErrorBuffer();
		Pointer<Pointer<DrmaaC.drmaa_job_template_s>> jtPointer = Pointer
				.allocatePointer(DrmaaC.drmaa_job_template_s.class);
		int result = drmaa_allocate_job_template(
				jtPointer,
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_NO_ACTIVE_SESSION:
				e = new NoActiveSessionException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
		return new JobTemplate(jtPointer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteJobTemplate(org.ggf.drmaa.JobTemplate jobTemplate)
			throws DrmaaException {
		if (jobTemplate == null) {
			throw new InvalidJobTemplateException(
					"The passed Job Template must not be null.");
		}
		if (!(jobTemplate instanceof JobTemplate)) {
			throw new InvalidJobTemplateException("Invalid Job Template. "
					+ "Use createJobTemplate() to allocate new Job Template");
		}
		JobTemplate jt = (JobTemplate) jobTemplate;
		Buffer eb = newErrorBuffer();
		int result = drmaa_delete_job_template(
				jt.getJtPointer().get(),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_NO_ACTIVE_SESSION:
				e = new NoActiveSessionException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String runJob(org.ggf.drmaa.JobTemplate jobTemplate)
			throws DrmaaException {
		if (jobTemplate == null) {
			throw new InvalidJobTemplateException(
					"The passed Job Template must not be null.");
		}
		if (!(jobTemplate instanceof JobTemplate)) {
			throw new InvalidJobTemplateException("Invalid Job Template. "
					+ "Use createJobTemplate() to allocate new Job Template");
		}
		JobTemplate jt = (JobTemplate) jobTemplate;
		Buffer eb = newErrorBuffer();
		Buffer jib = newJobIdBuffer();
		int result = drmaa_run_job(jib.getPointer(), jib.getLength(), jt
				.getJtPointer().get(), eb.getPointer(), eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_NO_ACTIVE_SESSION:
				e = new NoActiveSessionException(errorMessage);
				break;
			case DRMAA_ERRNO_TRY_LATER:
				e = new TryLaterException(errorMessage);
				break;
			case DRMAA_ERRNO_DENIED_BY_DRM:
				e = new DeniedByDrmException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
		return jib.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List runBulkJobs(org.ggf.drmaa.JobTemplate jobTemplate, int start,
			int end, int incr) throws DrmaaException {
		if (jobTemplate == null) {
			throw new InvalidJobTemplateException(
					"The passed Job Template must not be null.");
		}
		if (!(jobTemplate instanceof JobTemplate)) {
			throw new InvalidJobTemplateException("Invalid Job Template. "
					+ "Use createJobTemplate() to allocate new Job Template");
		}
		JobTemplate jt = (JobTemplate) jobTemplate;
		Buffer eb = newErrorBuffer();
		Pointer<Pointer<DrmaaC.drmaa_job_ids_s>> jIdsPointer = Pointer
				.allocatePointer(DrmaaC.drmaa_job_ids_s.class);
		int result = drmaa_run_bulk_jobs(
				jIdsPointer,
				jt.getJtPointer().get(),
				start,
				end,
				incr,
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_NO_ACTIVE_SESSION:
				e = new NoActiveSessionException(errorMessage);
				break;
			case DRMAA_ERRNO_TRY_LATER:
				e = new TryLaterException(errorMessage);
				break;
			case DRMAA_ERRNO_DENIED_BY_DRM:
				e = new DeniedByDrmException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
		List<String> jobIdsList = Collections.emptyList();
		Pointer<SizeT> numOfJobsPt = allocateSizeT();
		drmaa_get_num_job_ids(jIdsPointer.get(), numOfJobsPt);
		int numOfJobs = numOfJobsPt.get().intValue();
		if (numOfJobs > 0) {
			jobIdsList = new ArrayList<String>(numOfJobs);
			int nextJidResult;
			do {
				Buffer jib = newJobIdBuffer();
				nextJidResult = drmaa_get_next_job_id(
						jIdsPointer.get(),
						jib.getPointer(),
						jib.getLength());
				jobIdsList.add(jib.toString());
			} while (nextJidResult != DRMAA_ERRNO_NO_MORE_ELEMENTS);
			drmaa_release_job_ids(jIdsPointer.get());
		}
		return jobIdsList;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void control(String jobId, int action) throws DrmaaException {
		Buffer eb = newErrorBuffer();
		int result = drmaa_control(
				pointerToCString(jobId),
				action,
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_NO_ACTIVE_SESSION:
				e = new NoActiveSessionException(errorMessage);
				break;
			case DRMAA_ERRNO_RESUME_INCONSISTENT_STATE:
				e = new ResumeInconsistentStateException(errorMessage);
				break;
			case DRMAA_ERRNO_SUSPEND_INCONSISTENT_STATE:
				e = new SuspendInconsistentStateException(errorMessage);
				break;
			case DRMAA_ERRNO_HOLD_INCONSISTENT_STATE:
				e = new HoldInconsistentStateException(errorMessage);
				break;
			case DRMAA_ERRNO_RELEASE_INCONSISTENT_STATE:
				e = new ReleaseInconsistentStateException(errorMessage);
				break;
			case DRMAA_ERRNO_INVALID_JOB:
				e = new InvalidJobException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void synchronize(List jobIds, long timeout, boolean dispose)
			throws DrmaaException {
		Pointer<Pointer<Byte>> jobIdsPointer = pointerToCStrings((String[]) jobIds
				.toArray());
		Buffer eb = newErrorBuffer();
		int result = drmaa_synchronize(
				jobIdsPointer,
				timeout,
				dispose ? 1 : 0,
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_NO_ACTIVE_SESSION:
				e = new NoActiveSessionException(errorMessage);
				break;
			case DRMAA_ERRNO_RESUME_INCONSISTENT_STATE:
				e = new ResumeInconsistentStateException(errorMessage);
				break;
			case DRMAA_ERRNO_SUSPEND_INCONSISTENT_STATE:
				e = new SuspendInconsistentStateException(errorMessage);
				break;
			case DRMAA_ERRNO_HOLD_INCONSISTENT_STATE:
				e = new HoldInconsistentStateException(errorMessage);
				break;
			case DRMAA_ERRNO_RELEASE_INCONSISTENT_STATE:
				e = new ReleaseInconsistentStateException(errorMessage);
				break;
			case DRMAA_ERRNO_INVALID_JOB:
				e = new InvalidJobException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobInfo wait(String jobId, long timeout) throws DrmaaException {
		Buffer jib = newJobIdBuffer();
		Pointer<Integer> stat = allocateInt();
		Pointer<Pointer<drmaa_attr_values_s>> rusage = allocatePointer(DrmaaC.drmaa_attr_values_s.class);
		Buffer eb = newErrorBuffer();
		int result = drmaa_wait(
				pointerToCString(jobId),
				jib.getPointer(),
				jib.getLength(),
				stat,
				timeout,
				rusage,
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_NO_ACTIVE_SESSION:
				e = new NoActiveSessionException(errorMessage);
				break;
			case DRMAA_ERRNO_EXIT_TIMEOUT:
				e = new ExitTimeoutException(errorMessage);
				break;
			case DRMAA_ERRNO_INVALID_JOB:
				e = new InvalidJobException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
		return new JobInfo(jib.toString(), stat, rusage.get());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getJobProgramStatus(String jobId) throws DrmaaException {
		Pointer<Integer> jobStatus = allocateInt();
		Buffer eb = newErrorBuffer();
		int result = drmaa_job_ps(
				pointerToCString(jobId),
				jobStatus,
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			DrmaaException e;
			switch (result) {
			case DRMAA_ERRNO_NO_ACTIVE_SESSION:
				e = new NoActiveSessionException(errorMessage);
				break;
			case DRMAA_ERRNO_INVALID_JOB:
				e = new InvalidJobException(errorMessage);
				break;
			default:
				e = new PlainDrmaaException(errorMessage);
				break;
			}
			throw e;
		}
		return jobStatus.getInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getContact() {
		Buffer cb = Buffer.newContactBuffer();
		Buffer eb = newErrorBuffer();
		int result = drmaa_get_contact(
				cb.getPointer(),
				cb.getLength(),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			throw new IllegalStateException(errorMessage);
		}
		return cb.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Version getVersion() {
		Pointer<Integer> major = allocateInt();
		Pointer<Integer> minor = allocateInt();
		Buffer eb = newErrorBuffer();
		int result = drmaa_version(
				major,
				minor,
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			throw new IllegalStateException(errorMessage);
		}
		return new Version(major.getInt(), minor.getInt());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDrmSystem() {
		Buffer dsb = Buffer.newDrmSystemBuffer();
		Buffer eb = newErrorBuffer();
		int result = drmaa_get_DRM_system(
				dsb.getPointer(),
				dsb.getLength(),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			throw new IllegalStateException(errorMessage);
		}
		return dsb.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDrmaaImplementation() {
		Buffer dib = Buffer.newDrmImplementationBuffer();
		Buffer eb = newErrorBuffer();
		int result = drmaa_get_DRMAA_implementation(
				dib.getPointer(),
				dib.getLength(),
				eb.getPointer(),
				eb.getLength());
		if (result != DRMAA_ERRNO_SUCCESS) {
			String errorMessage = eb.toString();
			throw new IllegalStateException(errorMessage);
		}
		return dib.toString();
	}
}
