package mesosphere.marathon.state.schedule.engine

import akka.actor.{ Props, Actor, ActorLogging, ActorRef }
import mesosphere.marathon.state.schedule.engine.CronDispatcher.RunJob

abstract class CronDispatcher(
  jobLauncher: JobLauncher,
  launchCommand: LaunchCommand,
  cronExpr: String,
  cronScheduler: ActorRef)
    extends Actor with ActorLogging {

  def cronParser: CronParser

  override def preStart(): Unit = {
    cronParser.parseAndSchedule(cronExpr, self, RunJob)
  }

  import context.dispatcher

  override def receive: Receive = idle

  def idle: Receive = {
    case RunJob =>
      import akka.pattern.pipe
      jobLauncher.launch(launchCommand).pipeTo(self)
      context.become(active)
  }

  def active: Receive = {

    case lr: LaunchResult =>
      lr match {
        case LaunchFailed(pathId, message) =>
          onLaunchFailed()
          log.warning(s"Job launch failed. pathId: $pathId, message $message")
          cronScheduler ! lr

        case LaunchSuccessful =>
          onLaunchSuccessful()
          context.become(idle)
      }

    case RunJob =>
      log.warning("I missed the deadline!")
  }

  def onLaunchSuccessful(): Unit = {}
  def onLaunchFailed(): Unit = {}
}

object CronDispatcher {

  def props(
    jobLauncher: JobLauncher,
    launchCommand: LaunchCommand,
    cronExpr: String,
    cronScheduler: ActorRef): Props = Props(
    new CronDispatcher(jobLauncher, launchCommand, cronExpr, cronScheduler) {
      override def cronParser: CronParser = new NotProductionReadyCronParser
    }
  )

  case object RunJob

  case object Stop
}

trait CronParser {

  def parseAndSchedule[T](cronExpr: String, recipient: ActorRef, message: T): Unit

}

class NotProductionReadyCronParser extends CronParser {
  override def parseAndSchedule[T](cronExpr: String, recipient: ActorRef, message: T): Unit = {}
}