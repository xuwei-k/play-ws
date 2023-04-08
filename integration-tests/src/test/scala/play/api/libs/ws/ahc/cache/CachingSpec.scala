/*
 * Copyright (C) Lightbend Inc. <https://www.lightbend.com>
 */

package play.api.libs.ws.ahc.cache

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Route
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.AnyWordSpec
import play.AkkaServerProvider
import play.api.libs.ws.ahc._
import play.api.libs.ws.DefaultBodyReadables._
import play.shaded.ahc.org.asynchttpclient._

import scala.concurrent.Future
import scala.reflect.ClassTag

class CachingSpec extends AnyWordSpec with AkkaServerProvider with ScalaFutures {

  private def mock[A](implicit a: ClassTag[A]): A =
    Mockito.mock(a.runtimeClass).asInstanceOf[A]

  val asyncHttpClient: AsyncHttpClient = {
    val config                           = AhcWSClientConfigFactory.forClientConfig()
    val ahcConfig: AsyncHttpClientConfig = new AhcConfigBuilder(config).build()
    new DefaultAsyncHttpClient(ahcConfig)
  }

  override val routes: Route = {
    import akka.http.scaladsl.server.Directives._
    path("hello") {
      respondWithHeader(RawHeader("Cache-Control", "public")) {
        val httpEntity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>")
        complete(httpEntity)
      }
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    asyncHttpClient.close()
  }

  "GET" should {

    "work once" in {
      val cache = mock[Cache]
      when(cache.get(any[EffectiveURIKey]())).thenReturn(Future.successful(None))

      val cachingAsyncHttpClient = new CachingAsyncHttpClient(asyncHttpClient, new AhcHttpCache(cache))
      val ws                     = new StandaloneAhcWSClient(cachingAsyncHttpClient)

      ws.url(s"http://localhost:$testServerPort/hello")
        .get()
        .map { response =>
          assert(response.body[String] == "<h1>Say hello to akka-http</h1>")
        }
        .futureValue

      Mockito.verify(cache).get(EffectiveURIKey("GET", new java.net.URI(s"http://localhost:$testServerPort/hello")))
    }
  }
}
