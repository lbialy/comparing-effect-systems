package ma.chinespirit.crawldown

import munit.FunSuite
import sttp.tapir.*
import sttp.tapir.files.*
import sttp.tapir.server.jdkhttp.JdkHttpServer
import com.sun.net.httpserver.HttpServer
import java.util.concurrent.Executors
import java.net.InetSocketAddress
import scala.util.{Try, Success, Failure}
import java.time.{Instant, Duration => JDuration}
import scala.concurrent.duration.Duration

case class TimingStats(
    variant: String,
    average: Double,
    min: Double,
    max: Double,
    variance: Double,
    stdDev: Double,
    successfulRuns: Int,
    failedRuns: Int
)

// Control the number of iterations with the ITERATIONS environment variable!
class PerfSuite extends FunSuite:

  override def munitTimeout: Duration = Duration.Inf

  var address: InetSocketAddress = null
  var server: HttpServer = null

  val OxDocsCount = 50

  override def beforeAll(): Unit =
    os.remove.all(os.pwd / "ox_docs")
    os.proc("unzip", "-o", "ox_docs.zip").call()

    server = JdkHttpServer()
      .addEndpoint(staticFilesGetServerEndpoint(emptyInput)("ox_docs"))
      .host("localhost")
      .port(0)
      .executor(Executors.newVirtualThreadPerTaskExecutor())
      .start()

    address = server.getAddress
    println(s"Server is running at http://${address.getHostString}:${address.getPort}")

  override def afterAll(): Unit =
    os.remove.all(os.pwd / "ox_docs")
    server.stop(0)

  val variants = List(
    "future",
    "future-high",
    "cats",
    "cats-tf",
    "cats-high",
    "cats-high-tf",
    "zio",
    "zio-high",
    "kyo",
    "kyo-high",
    "ox",
    "ox-high",
    "gears",
    "gears-high"
  )

  val iterations = sys.env.get("ITERATIONS").map(_.toInt).getOrElse(1)
  val selector = Some("div[role=main]")

  def measureTime[T](block: => T): (T, Double) =
    val start = Instant.now()
    val result = block
    val end = Instant.now()
    val duration = JDuration.between(start, end).toMillis.toDouble
    (result, duration)

  def runVariantBench(variant: String, rootUrl: String): List[Double] =
    println(s"Cleaning up the scrape result directory: ${address.getHostString}")
    os.remove.all(os.pwd / address.getHostString)

    val timings = scala.collection.mutable.ListBuffer[Double]()
    var successCount = 0
    var failCount = 0

    println(s"\n🚀 Starting benchmark for variant: $variant")
    println(s"📊 Running $iterations iterations...")

    for i <- 1 to iterations do
      Try {
        val (_, timing) = measureTime {
          run(variant, rootUrl, selector)
        }
        timing
      } match {
        case Success(timing) =>
          val count = os.walk(os.pwd / address.getHostString).size
          if count != OxDocsCount then
            println(s"❌ $variant: $count files in the result directory, expected $OxDocsCount")
            failCount += 1
          else
            timings += timing
            successCount += 1

        case Failure(exception) =>
          println(s"❌ Failed run $i for $variant: ${exception}")
          exception.printStackTrace()
          failCount += 1
      }

      os.remove.all(os.pwd / address.getHostString)

      if i % 10 == 0 then
        val progress = (i.toDouble / iterations * 100).toInt
        val remaining = iterations - i
        println(s"✅ $variant: Completed $i/$iterations iterations ($progress%) - $remaining remaining")

    println(s"📈 $variant completed: $successCount successful, $failCount failed runs")
    timings.toList

  def calculateStats(variant: String, timings: List[Double]): TimingStats =
    if timings.isEmpty then TimingStats(variant, 0.0, 0.0, 0.0, 0.0, 0.0, 0, iterations)
    else
      val average = timings.sum / timings.length
      val min = timings.min
      val max = timings.max
      val variance = timings.map(t => math.pow(t - average, 2)).sum / timings.length
      val stdDev = math.sqrt(variance)

      TimingStats(variant, average, min, max, variance, stdDev, timings.length, iterations - timings.length)

  def printResultsTable(results: List[TimingStats]): Unit =
    println("\n" + "=" * 120)
    println("🏆 PERFORMANCE BENCHMARK RESULTS")
    println("=" * 120)

    val header =
      f"${"Variant"}%-20s ${"Avg (ms)"}%10s ${"Min (ms)"}%10s ${"Max (ms)"}%10s ${"Variance"}%12s ${"Std Dev"}%10s ${"Success"}%8s ${"Failed"}%8s"
    println(header)
    println("-" * 120)

    val sortedResults = results.filter(_.successfulRuns > 0).sortBy(_.average)

    sortedResults.foreach { stats =>
      println(
        f"${stats.variant}%-20s ${stats.average}%10.2f ${stats.min}%10.2f ${stats.max}%10.2f ${stats.variance}%12.2f ${stats.stdDev}%10.2f ${stats.successfulRuns}%8d ${stats.failedRuns}%8d"
      )
    }

    println("-" * 120)

    if sortedResults.nonEmpty then
      val fastest = sortedResults.head
      println(s"🥇 Fastest variant: ${fastest.variant} (${f"${fastest.average}%.2f"}ms average)")

      if sortedResults.length > 1 then
        val slowest = sortedResults.last
        val speedup = slowest.average / fastest.average
        println(s"🐌 Slowest variant: ${slowest.variant} (${f"${slowest.average}%.2f"}ms average)")
        println(s"📊 Speed difference: ${f"${speedup}%.2f"}x")

    val failedVariants = results.filter(_.successfulRuns == 0)
    if failedVariants.nonEmpty then println(s"\n❌ Failed variants: ${failedVariants.map(_.variant).mkString(", ")}")

  test("performance benchmark all variants") {
    val rootUrl = s"http://${address.getHostString}:${address.getPort}/"

    println(s"\n🔥 STARTING COMPREHENSIVE PERFORMANCE BENCHMARK")
    println(s"🎯 Testing ${variants.length} variants with $iterations iterations each")
    println(s"🌐 Root URL: $rootUrl")
    println(s"🎛️ Selector: ${selector.getOrElse("none")}")

    val allResults = variants.map { variant =>
      val timings = runVariantBench(variant, rootUrl)
      calculateStats(variant, timings)
    }

    printResultsTable(allResults)

    // Ensure at least one variant succeeded
    val anySuccess = allResults.exists(_.successfulRuns > 0)
    assert(anySuccess, "At least one variant should complete successfully")
  }
