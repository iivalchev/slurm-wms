package bg.tusofia.cs.slurm.wms.samples

import org.ggf.drmaa.SessionFactory
import org.ggf.drmaa.Session._


/**
 * Created by ivan on 5/7/14.
 */
object JobSubmissionDemo extends App {
  System.setProperty("org.ggf.drmaa.SessionFactory", "bg.tusofia.cs.drmaa.SessionFactory");

  val session = {
    val factory = SessionFactory.getFactory
    factory getSession
  }
  try {
    session.init("")

    val template = session.createJobTemplate()
    template.setRemoteCommand("/media/share/bin/cpi_mc.sh")
    template.setOutputPath(":/media/share/out/cpi_mc.out")

    val jobId = session.runJob(template)
    Console.println("Job Id = " + jobId)

    val jobInfo = session.wait(jobId, TIMEOUT_WAIT_FOREVER)
    if (jobInfo.hasExited) {
      Console.println("Exit Status = " + jobInfo.getExitStatus)
    }
    Console.println("Resource Usage = " + jobInfo.getResourceUsage)
  } finally {
    session.exit();
  }
}
