/*
 * Copyright (C) Lightbend Inc. <https://www.lightbend.com>
 */

package play

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.stream.Materializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.Millis
import org.scalatest.time.Span

trait AkkaServerProvider extends BeforeAndAfterAll with ScalaFutures { self: Suite =>

  implicit override def patienceConfig: PatienceConfig =
    PatienceConfig(Span(5000, Millis))

  /**
   * @return Routes to be used by the test.
   */
  def routes: Route

  protected implicit def executionContext: ExecutionContext = ExecutionContext.global

  var testServerPort: Int            = _
  val defaultTimeout: FiniteDuration = 5.seconds

  // Create Akka system for thread and streaming management
  implicit val system: ActorSystem        = ActorSystem()
  implicit val materializer: Materializer = Materializer.matFromSystem

  lazy val futureServer: Future[Http.ServerBinding] = {
    // Using 0 (zero) means that a random free port will be used.
    // So our tests can run in parallel and won't mess with each other.
    Http().bindAndHandle(routes, "localhost", 0)
  }

  override def beforeAll(): Unit = {
    val portFuture = futureServer.map(_.localAddress.getPort)(executionContext)
    portFuture.foreach(port => testServerPort = port)(executionContext)
    Await.ready(portFuture, defaultTimeout)
  }

  override def afterAll(): Unit = {
    futureServer.foreach(_.unbind())(executionContext)
    val terminate = system.terminate()
    Await.ready(terminate, defaultTimeout)
  }
}
