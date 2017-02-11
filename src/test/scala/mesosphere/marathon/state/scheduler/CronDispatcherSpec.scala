package mesosphere.marathon.state.scheduler

import akka.actor.{ Props, ActorRef }
import akka.testkit.TestProbe
import mesosphere.marathon.state.{ PathId, AppDefinition }
import mesosphere.marathon.state.schedule.engine._
import mesosphere.AkkaUnitTest
import akka.Done
import concurrent.duration._

import scala.concurrent.{ Future, Promise }

class CronDispatcherSpec extends AkkaUnitTest {

  val runSpec = AppDefinition(id = PathId("/test"))

  type CronExpr = String

  class ProbeBasedCronParser {

    val crExpr = "test expression"

    val cronParserProbe = TestProbe()

    val parseAndSchedulePromise = Promise[(CronExpr, ActorRef, Any)]()

    val promiseCronParser = new CronParser {
      override def parseAndSchedule[T](cronExpr: String, recipient: ActorRef, message: T): Unit = {
        parseAndSchedulePromise.success((cronExpr, recipient, message))
      }
    }

    val jobLauncherProbe = TestProbe()
    val jobLauncher = new JobLauncher {
      override def launch(launchCommand: LaunchCommand): Future[LaunchResult] = {
        val launchResultPromise = Promise[LaunchResult]()
        jobLauncherProbe.ref ! launchResultPromise
        launchResultPromise.future
      }
    }

    val schedulerProbe = TestProbe()

    val onLaunchSuccessfulProbe = TestProbe()
    val onLaunchFailedProbe = TestProbe()
    val cronDispatcherActor = system.actorOf(Props(new CronDispatcher(
      jobLauncher, RunSpecLaunchCommand(runSpec), crExpr, schedulerProbe.ref
    ) {
      override def cronParser: CronParser = promiseCronParser

      override def onLaunchSuccessful(): Unit = {
        onLaunchFailedProbe.ref ! Done
      }

      override def onLaunchFailed(): Unit = {
        onLaunchFailedProbe.ref ! Done
      }

    }))

  }

  "CronDispatcher" should {
    "call CronParser on start given the cron expression (old)" ignore {

      val p = Promise[Done]()

      val crExpr = "test expression"

      val cp = new CronParser {
        override def parseAndSchedule[T](cronExpr: String, recipient: ActorRef, message: T): Unit = {
          if (cronExpr == crExpr) {
            p.trySuccess(Done)
          } else {
            p.failure(new Exception("cron expression is not the same"))
          }
        }
      }

      val joblauncher = new JobLauncher {
        override def launch(launchCommand: LaunchCommand): Future[LaunchResult] = Future.successful(LaunchSuccessful)
      }

      val schedulerProbe = TestProbe()

      system.actorOf(Props(new CronDispatcher(joblauncher, RunSpecLaunchCommand(runSpec), crExpr, schedulerProbe.ref) {
        override def cronParser: CronParser = cp
      }))

      p.future.futureValue shouldEqual Done

    }

    "call CronParser on start given the cron expression" in new ProbeBasedCronParser {

      parseAndSchedulePromise.future.futureValue._1 shouldEqual crExpr

      system.stop(cronDispatcherActor)

    }

    "correctly run the happy path when RunJob message received" in new ProbeBasedCronParser {

      jobLauncherProbe.expectNoMsg(100.millis)

      cronDispatcherActor ! CronDispatcher.RunJob

      val jobPromise = jobLauncherProbe.receiveOne(1.second).asInstanceOf[Promise[LaunchResult]]

      jobPromise.success(LaunchSuccessful)

      onLaunchSuccessfulProbe.receiveOne(1.second) shouldEqual Done

    }

  }

}
