package mesosphere.marathon.state.schedule.engine

import akka.actor.{ ActorRef, Props, Actor }
import mesosphere.marathon.state.schedule.Periodic
import mesosphere.marathon.state.{ PathId, RunSpec }
import CronScheduler._

abstract class CronScheduler extends Actor {

  var addedRunSpecsIds: Map[PathId, ActorRef] = Map.empty

  def jobLauncher: JobLauncher

  def cronDispatcherProps(launchCommand: LaunchCommand, cronExpr: String): Props = {
    CronDispatcher.props(jobLauncher, launchCommand, cronExpr, self)
  }

  override def receive: Receive = {

    case AddRunSpec(runSpec) =>

      validateRunSpec(runSpec)(
        onValid = { periodic =>

        addedRunSpecsIds.get(runSpec.id) match {
          case Some(actorRef) =>
            actorRef ! CronDispatcher.Stop
            val dispatcher = context.actorOf(cronDispatcherProps(RunSpecLaunchCommand(runSpec), periodic.cron))
            addedRunSpecsIds += runSpec.id -> dispatcher

          case None =>
            val dispatcher = context.actorOf(cronDispatcherProps(RunSpecLaunchCommand(runSpec), periodic.cron))
            addedRunSpecsIds += runSpec.id -> dispatcher

        }

      }, onInvalid = {
        sender() ! RunSpecAdditionFailed(runSpec.id, "scheduling strategy is not a cron expression")
      })

    case LaunchFailed(pathId, reason) =>

  }

  def validateRunSpec(runSpec: RunSpec)(onValid: (Periodic) => Unit, onInvalid: => Unit): Unit = {
    runSpec.schedule.strategy match {
      case p: Periodic => onValid(p)
      case _ => onInvalid
    }
  }

}

object CronScheduler {
  sealed trait Command
  case class AddRunSpec(runSpec: RunSpec) extends Command

  sealed trait Response
  case class RunSpecAddedSuccessfully(pathId: PathId) extends Response
  case class RunSpecAdditionFailed(pathId: PathId, reason: String) extends Response

}