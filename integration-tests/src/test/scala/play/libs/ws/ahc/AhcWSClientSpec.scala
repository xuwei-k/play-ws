/*
 * Copyright (C) Lightbend Inc. <https://www.lightbend.com>
 */

package play.libs.ws.ahc

import akka.http.scaladsl.server.Route
import akka.stream.javadsl.Sink
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.AnyWordSpec
import play.AkkaServerProvider
import play.libs.ws._

import scala.jdk.FutureConverters._
import scala.concurrent.Future

class AhcWSClientSpec
    extends AnyWordSpec
    with AkkaServerProvider
    with StandaloneWSClientSupport
    with ScalaFutures
    with XMLBodyWritables
    with XMLBodyReadables {

  override val routes: Route = {
    import akka.http.scaladsl.server.Directives._
    get {
      complete("<h1>Say hello to akka-http</h1>")
    } ~
      post {
        entity(as[String]) { echo =>
          complete(echo)
        }
      }
  }

  "play.libs.ws.ahc.StandaloneAhcWSClient" should {

    "get successfully" in withClient() { client =>
      def someOtherMethod(string: String) = {
        new InMemoryBodyWritable(akka.util.ByteString.fromString(string), "text/plain")
      }
      client
        .url(s"http://localhost:$testServerPort")
        .post(someOtherMethod("hello world"))
        .asScala
        .map(response => assert(response.getBody() == "hello world"))
        .futureValue
    }

    "source successfully" in withClient() { client =>
      val future = client.url(s"http://localhost:$testServerPort").stream().asScala
      val result: Future[ByteString] = future.flatMap { (response: StandaloneWSResponse) =>
        response.getBodyAsSource.runWith(Sink.head[ByteString](), materializer).asScala
      }
      val expected: ByteString = ByteString.fromString("<h1>Say hello to akka-http</h1>")
      assert(result.futureValue == expected)
    }

    "round trip XML successfully" in withClient() { client =>
      val document = XML.fromString("""<?xml version="1.0" encoding='UTF-8'?>
                                      |<note>
                                      |  <from>hello</from>
                                      |  <to>world</to>
                                      |</note>""".stripMargin)
      document.normalizeDocument()

      client
        .url(s"http://localhost:$testServerPort")
        .post(body(document))
        .asScala
        .map { response =>
          import javax.xml.parsers.DocumentBuilderFactory
          val dbf = DocumentBuilderFactory.newInstance
          dbf.setNamespaceAware(true)
          dbf.setCoalescing(true)
          dbf.setIgnoringElementContentWhitespace(true)
          dbf.setIgnoringComments(true)
          dbf.newDocumentBuilder

          val responseXml = response.getBody(xml())
          responseXml.normalizeDocument()

          assert(responseXml.isEqualNode(document))
          assert(response.getUri == new java.net.URI(s"http://localhost:$testServerPort"))
        }
        .futureValue
    }
  }
}
