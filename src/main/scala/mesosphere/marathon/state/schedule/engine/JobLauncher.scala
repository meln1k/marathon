package mesosphere.marathon.state.schedule.engine

import mesosphere.marathon.state.{ PathId, RunSpec }

import scala.concurrent.Future

trait JobLauncher {

  def launch(launchCommand: LaunchCommand): Future[LaunchResult]

}

sealed trait LaunchCommand

case class RunSpecLaunchCommand(spec: RunSpec) extends LaunchCommand

sealed trait LaunchResult

case object LaunchSuccessful extends LaunchResult

case class LaunchFailed(pathId: PathId, reason: String) extends LaunchResult