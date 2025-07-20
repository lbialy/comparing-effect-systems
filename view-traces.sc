//> using dep com.softwaremill.sttp.tapir::tapir-files:1.11.37
//> using dep com.softwaremill.sttp.tapir::tapir-jdkhttp-server:1.11.37
//> using dep com.softwaremill.sttp.tapir::tapir-jsoniter-scala:1.11.37
//> using dep com.github.plokhotnyuk.jsoniter-scala::jsoniter-scala-core:2.36.7
//> using dep com.github.plokhotnyuk.jsoniter-scala::jsoniter-scala-macros:2.36.7
//> using dep com.lihaoyi::os-lib:0.11.4

import sttp.tapir.*
import sttp.tapir.files.*
import sttp.tapir.server.jdkhttp.JdkHttpServer
import sttp.tapir.json.jsoniter.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import java.util.concurrent.Executors

given JsonValueCodec[List[String]] = JsonCodecMaker.make

val tracesDirectory = os.pwd / "traces"

val handleTracesEndpoint =
  endpoint.get
    .in("list-traces")
    .out(jsonBody[List[String]])
    .handle { _ =>
      val traceFiles =
        if os.exists(tracesDirectory) then
          os.list(tracesDirectory)
            .filter(_.ext == "json")
            .map(file => file.last)
            .toList
            .sorted
        else List.empty[String]

      Right(traceFiles)
    }

val server = JdkHttpServer()
  .addEndpoint(handleTracesEndpoint)
  .addEndpoint(staticFilesGetServerEndpoint("traces")("traces"))
  .addEndpoint(staticFilesGetServerEndpoint(emptyInput)("tracing"))
  .host("localhost")
  .port(9999)
  .executor(Executors.newVirtualThreadPerTaskExecutor())
  .start()

val address = server.getAddress
println(s"Server is running at http://${address.getHostString}:${address.getPort}")

// Keep the server running
scala.io.StdIn.readLine("Press ENTER to stop the server...")
server.stop(0)
