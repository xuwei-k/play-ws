/*
 * Copyright (C) Lightbend Inc. <https://www.lightbend.com>
 */

package play.api.libs.ws.ahc

import akka.http.scaladsl.model.StatusCodes.Redirection
import akka.http.scaladsl.model.headers.HttpCookie
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.MissingCookieRejection
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.AnyWordSpec
import play.AkkaServerProvider
import play.api.libs.ws._
import play.shaded.ahc.org.asynchttpclient.handler.MaxRedirectException

import scala.concurrent._

class AhcWSClientSpec
    extends AnyWordSpec
    with AkkaServerProvider
    with StandaloneWSClientSupport
    with ScalaFutures
    with DefaultBodyReadables
    with DefaultBodyWritables {

  def withClientFollowingRedirect[A](
      config: AhcWSClientConfig = AhcWSClientConfigFactory.forConfig()
  )(block: StandaloneAhcWSClient => A): A = {
    withClient(
      config.copy(
        wsClientConfig = config.wsClientConfig.copy(followRedirects = true)
      )
    )(block)
  }

  val indexRoutes: Route = {
    path("index") {
      extractRequest { request =>
        respondWithHeaders(request.headers.map(h => RawHeader(s"Req-${h.name}", h.value))) {
          get {
            complete("Say hello to akka-http")
          } ~
            post {
              complete(s"POST: ${request.entity}")
            }
        }
      }
    }
  }

  val cookieRoutes: Route = {
    path("cookie") {
      get {
        setCookie(HttpCookie("flash", "redirect-cookie")) {
          redirect("/cookie-destination", StatusCodes.MovedPermanently)
        }
      }
    } ~
      path("cookie-destination") {
        get {
          optionalCookie("flash") {
            case Some(c) => complete(s"Cookie value => ${c.value}")
            case None    => reject(MissingCookieRejection("flash"))
          }
        }
      }
  }

  val redirectRoutes: Route = {
    // Single redirect
    path("redirect" / IntNumber) { status =>
      get {
        val redirectCode = StatusCode.int2StatusCode(status).asInstanceOf[Redirection]
        redirect("/index", redirectCode)
      } ~
        post {
          val redirectCode = StatusCode.int2StatusCode(status).asInstanceOf[Redirection]
          redirect("/index", redirectCode)
        }
    } ~
      path("redirects" / IntNumber / IntNumber) { (status, count) =>
        get {
          val redirectCode = StatusCode.int2StatusCode(status).asInstanceOf[Redirection]
          if (status == 1) redirect("/index", redirectCode)
          else redirect(s"/redirects/$status/${count - 1}", redirectCode)
        }
      }
  }

  override val routes: Route = indexRoutes ~ cookieRoutes ~ redirectRoutes

  "url" should {
    "throw an exception on invalid url" in {
      withClient() { client =>
        assertThrows[IllegalArgumentException] {
          client.url("localhost")
        }
      }
    }

    "not throw exception on valid url" in {
      withClient() { client =>
        client.url(s"http://localhost:$testServerPort")
      }
    }
  }

  "WSClient" should {

    "request a url as an in memory string" in {
      withClient() { client =>
        val result = Await.result(
          client.url(s"http://localhost:$testServerPort/index").get().map(res => res.body[String]),
          defaultTimeout
        )
        assert(result == "Say hello to akka-http")
      }
    }

    "request a url as a Foo" in {
      case class Foo(body: String)

      implicit val fooBodyReadable: BodyReadable[Foo] = BodyReadable[Foo] { response =>
        import play.shaded.ahc.org.asynchttpclient.{ Response => AHCResponse }
        val ahcResponse = response.asInstanceOf[StandaloneAhcWSResponse].underlying[AHCResponse]
        Foo(ahcResponse.getResponseBody)
      }

      withClient() { client =>
        val result = Await.result(
          client.url(s"http://localhost:$testServerPort/index").get().map(res => res.body[Foo]),
          defaultTimeout
        )
        assert(result == Foo("Say hello to akka-http"))
      }
    }

    "request a url as a stream" in {
      withClient() { client =>
        val resultSource = Await.result(
          client.url(s"http://localhost:$testServerPort/index").stream().map(_.bodyAsSource),
          defaultTimeout
        )
        val bytes: ByteString = Await.result(resultSource.runWith(Sink.head), defaultTimeout)
        assert(bytes.utf8String == "Say hello to akka-http")
      }
    }

    "when following redirect" should {

      "honor the number of redirects allowed" in {
        // 1. Default number of max redirects is 5
        withClientFollowingRedirect() { client =>
          assertThrows[MaxRedirectException] {
            val request = client
              // 2. Ask to redirect 10 times
              .url(s"http://localhost:$testServerPort/redirects/302/10")
              .get()
            Await.result(request, defaultTimeout)
          }
        }
      }

      "should follow a redirect when configured to" in {
        withClientFollowingRedirect() { client =>
          val result = Await.result(
            client.url(s"http://localhost:$testServerPort/redirect/302").get().map(res => res.body[String]),
            defaultTimeout
          )
          assert(result == "Say hello to akka-http")
        }
      }

      "should not follow redirect if client is configured not to" in {
        val wsConfig    = WSClientConfig().copy(followRedirects = false)
        val ahcWsConfig = AhcWSClientConfigFactory.forConfig().copy(wsClientConfig = wsConfig)
        withClient(config = ahcWsConfig) { client =>
          val result = Await.result(client.url(s"http://localhost:$testServerPort/redirect/302").get(), defaultTimeout)
          assert(result.status == 302)
        }
      }

      "should not follow redirect if request is configured not to" in {
        // 1. Client is configured to follow the redirect
        withClientFollowingRedirect() { client =>
          val request = client
            .url(s"http://localhost:$testServerPort/redirect/302")
            // 2. But turn off follow redirects for this request
            .withFollowRedirects(false)
            .get()
          val result = Await.result(request, defaultTimeout)
          assert(result.status == 302)
        }
      }

      "follow redirect for HTTP 301 Moved Permanently" in {
        withClientFollowingRedirect() { client =>
          val result = Await.result(
            client.url(s"http://localhost:$testServerPort/redirect/301").get().map(res => res.body[String]),
            defaultTimeout
          )
          assert(result == "Say hello to akka-http")
        }
      }

      "follow redirect for HTTP 302 Found" in {
        withClientFollowingRedirect() { client =>
          val result = Await.result(
            client.url(s"http://localhost:$testServerPort/redirect/302").get().map(res => res.body[String]),
            defaultTimeout
          )
          assert(result == "Say hello to akka-http")
        }
      }

      "follow redirect for HTTP 303 See Other" in {
        withClientFollowingRedirect() { client =>
          val result = Await.result(
            client.url(s"http://localhost:$testServerPort/redirect/303").get().map(res => res.body[String]),
            defaultTimeout
          )
          assert(result == "Say hello to akka-http")
        }
      }

      "follow redirect for HTTP 307 Temporary Redirect" in {
        withClientFollowingRedirect() { client =>
          val result = Await.result(
            client.url(s"http://localhost:$testServerPort/redirect/307").get().map(res => res.body[String]),
            defaultTimeout
          )
          assert(result == "Say hello to akka-http")
        }
      }

      "follow redirect for HTTP 308 Permanent Redirect" in {
        withClientFollowingRedirect() { client =>
          val result = Await.result(
            client.url(s"http://localhost:$testServerPort/redirect/308").get().map(res => res.body[String]),
            defaultTimeout
          )
          assert(result == "Say hello to akka-http")
        }
      }

      "keep virtual host" in {
        withClientFollowingRedirect() { client =>
          val request = client
            .url(s"http://localhost:$testServerPort/redirect/303")
            .withVirtualHost("localhost1")
            .get()
          val result = Await.result(request, defaultTimeout)
          assert(result.header("Req-Host") == Some("localhost1"))
        }
      }

      "keep http headers" in {
        withClientFollowingRedirect() { client =>
          val request = client
            .url(s"http://localhost:$testServerPort/redirect/303")
            .addHttpHeaders("X-Test" -> "Test")
            .get()
          val result = Await.result(request, defaultTimeout)
          assert(result.header("Req-X-Test") == Some("Test"))
        }
      }

      "keep auth information" in {
        withClientFollowingRedirect() { client =>
          val request = client
            .url(s"http://localhost:$testServerPort/redirect/303")
            .withAuth("test", "test", WSAuthScheme.BASIC)
            .get()
          val result = Await.result(request, defaultTimeout)
          assert(result.header("Req-Authorization").isDefined)
        }
      }

      "keep cookie when following redirect automatically and cookie store is configured" in {
        withClientFollowingRedirect(AhcWSClientConfigFactory.forConfig().copy(useCookieStore = true)) { client =>
          val result = Await.result(
            client.url(s"http://localhost:$testServerPort/cookie").get().map(res => res.body[String]),
            defaultTimeout
          )
          assert(result == s"Cookie value => redirect-cookie")
        }
      }

      "not keep cookie when repeating the request" in {
        withClientFollowingRedirect() { client =>
          // First let's do a request that sets a cookie
          Await.result(
            client.url(s"http://localhost:$testServerPort/cookie").get().map(res => res.body[String]),
            defaultTimeout
          )

          // Then run a request to a url that checks the cookie, but without setting it
          val res2 = Await.result(
            client.url(s"http://localhost:$testServerPort/cookie-destination").get().map(res => res.body[String]),
            defaultTimeout
          )
          assert(res2 == s"Request is missing required cookie 'flash'")
        }
      }

      "switch to get " should {
        "for HTTP 301 Moved Permanently" in {
          withClientFollowingRedirect() { client =>
            val request = client
              .url(s"http://localhost:$testServerPort/redirect/301")
              // 1. We are doing a POST to a URL that will redirect to a path
              // That only accepts GETs
              .post("request body")

            val result = Await.result(request.map(res => res.body[String]), defaultTimeout)

            // 2. So when following the redirect, the GET path should be found
            // and we get its body
            assert(result == "Say hello to akka-http")
          }
        }

        "for HTTP 303 See Other" in {
          withClientFollowingRedirect() { client =>
            val request = client
              .url(s"http://localhost:$testServerPort/redirect/303")
              .post("request body")
            val result = Await.result(request.map(res => res.body[String]), defaultTimeout)
            assert(result == "Say hello to akka-http")
          }
        }

        "for HTTP 302 Found and not strict handling" in {
          withClientFollowingRedirect() { client =>
            val request = client
              .url(s"http://localhost:$testServerPort/redirect/302")
              // 1. We are doing a POST to a URL that will redirect to a path
              // That only accepts GETs
              .post("request body")

            val result = Await.result(request.map(res => res.body[String]), defaultTimeout)

            // 2. So when following the redirect, the GET path should be found
            // and we get its body
            assert(result == "Say hello to akka-http")
          }
        }
      }
    }

  }
}
