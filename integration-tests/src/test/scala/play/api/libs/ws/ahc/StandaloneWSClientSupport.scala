/*
 * Copyright (C) Lightbend Inc. <https://www.lightbend.com>
 */

package play.api.libs.ws.ahc

import akka.stream.Materializer

trait StandaloneWSClientSupport {

  def materializer: Materializer

  def withClient[A](
      config: AhcWSClientConfig = AhcWSClientConfigFactory.forConfig()
  )(block: StandaloneAhcWSClient => A): A = {
    val client = StandaloneAhcWSClient(config)(materializer)
    try {
      block(client)
    } finally {
      client.close()
    }
  }
}
