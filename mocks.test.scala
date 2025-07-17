package ma.chinespirit.crawldown

import cats.effect.Async
import cats.effect.IO
import cats.effect.Sync
import cats.syntax.all.*
import kyo.{Async as _, Result as _, Sync as KyoSync, *}
import sttp.model.Uri
import zio.Task as ZTask
import zio.ZIO

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import Uri.*

object Templating:
  def page(links: Set[String]): String =
    s"""
     |<html>
     |<head></head>
     |<body>
     |<!-- keep links absolute - prepend / to each link -->
     |${links.map(link => s"  <a href=\"/$link\">$link</a>").mkString("\n")}
     |</body>
     |</html>""".stripMargin

trait MockMetadata:
  def visited: Vector[String]
  def hasDuplicates: Boolean = visited.toSet.size != visited.size
  def maxParallelism: Int

case class PageSpec(
    links: Set[String],
    delay: Option[Duration] = None,
    error: Option[Exception] = None
)

trait ScraperAdapter:
  def maxParallelism: Int
  def visited: Vector[String]
  def hasDuplicates: Boolean
  def run: Unit

def makeScrapers(
    routes: Map[String, PageSpec],
    scraperParallelism: Int,
    scraperQueueCapacity: Int,
    scrapeMaxDepth: Int,
    futureEC: ExecutionContext
): Map[String, ScraperAdapter] =
  def makeFutureScraper: ScraperAdapter =
    val services = makeFutureServices(using futureEC)
    val scraper = FutureScraper(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism,
      queueCapacity = scraperQueueCapacity
    )(using futureEC)

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit = scraper.start()

  def makeFutureScraperHighLevel: ScraperAdapter =
    val services = makeFutureServices(using futureEC)
    val scraper = FutureScraperHighLevel(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism
    )(using futureEC)

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit = scraper.start()

  def makeCatsScraperHighLevel: ScraperAdapter =
    val services = makeCatsServices
    val scraper = CatsScraperHighLevel(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit =
        import cats.effect.unsafe.implicits.global
        scraper.start.unsafeRunSync()

  def makeCatsScraper: ScraperAdapter =
    val services = makeCatsServices
    val scraper = CatsScraper(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism,
      queueCapacity = scraperQueueCapacity
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit =
        import cats.effect.unsafe.implicits.global
        scraper.start.unsafeRunSync()

  def makeCatsScraperTF: ScraperAdapter =
    val services = makeCatsServicesTF[IO]
    val scraper = CatsScraperTF(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism,
      queueCapacity = scraperQueueCapacity
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit =
        import cats.effect.unsafe.implicits.global
        scraper.start.unsafeRunSync()

  def makeCatsScraperHighTF: ScraperAdapter =
    val services = makeCatsServicesTF[IO]
    val scraper = CatsScraperHighLevelTF(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit =
        import cats.effect.unsafe.implicits.global
        scraper.start.unsafeRunSync()

  def makeZioScraperHighLevel: ScraperAdapter =
    val services = makeZioServices
    val scraper = ZIOScraperHighLevel(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit =
        zio.Unsafe.unsafely {
          zio.Runtime.default.unsafe.run(scraper.start).getOrThrow()
        }

  def makeZioScraper: ScraperAdapter =
    val services = makeZioServices
    val scraper = ZIOScraper(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit =
        zio.Unsafe.unsafely {
          zio.Runtime.default.unsafe.run(scraper.start).getOrThrow()
        }

  def makeOxScraper: ScraperAdapter =
    val services = makeSyncServices
    val scraper = OxScraper(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism,
      queueCapacity = scraperQueueCapacity
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit = scraper.start()

  def makeOxScraperHighLevel: ScraperAdapter =
    val services = makeSyncServices
    val scraper = OxScraperHighLevel(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit = scraper.start()

  def makeKyoScraper: ScraperAdapter =
    val services = makeKyoServices
    val scraper = KyoScraper(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit =
        import kyo.AllowUnsafe.embrace.danger
        import kyo.*
        KyoApp.Unsafe.runAndBlock(kyo.Duration.Infinity)(scraper.start).getOrThrow

  def makeKyoScraperHighLevel: ScraperAdapter =
    val services = makeKyoServices
    val scraper = KyoScraperHighLevel(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit =
        import kyo.AllowUnsafe.embrace.danger
        import kyo.*
        KyoApp.Unsafe.runAndBlock(kyo.Duration.Infinity)(scraper.start).getOrThrow

  def makeGearsScraper: ScraperAdapter =
    val services = makeSyncServices
    val scraper = GearsScraper(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit = scraper.start()

  def makeGearsScraperHighLevel: ScraperAdapter =
    val services = makeSyncServices
    val scraper = GearsScraperHighLevel(
      fetch = services,
      store = services,
      root = uri"http://localhost:8080/",
      selector = None,
      maxDepth = scrapeMaxDepth,
      parallelism = scraperParallelism
    )

    new ScraperAdapter:
      def maxParallelism: Int = services.maxParallelism
      def visited: Vector[String] = services.visited
      def hasDuplicates: Boolean = services.hasDuplicates
      def run: Unit = scraper.start()

  def normalisePath(uri: Uri): String =
    uri.path.mkString("/") match
      case ""   => "index"
      case path => path

  def makeFutureServices(using ExecutionContext): Fetch[Future] & Store[Future] & MockMetadata =
    new Fetch[Future] with Store[Future] with MockMetadata:
      val visitedUris = AtomicReference(Vector.empty[String])
      val maxSeen = AtomicInteger(0)
      val currentPending = AtomicInteger(0)

      def fetch(uri: Uri)(using trace: Trace): Future[String] =
        val path = normalisePath(uri)
        routes.get(path) match
          case Some(PageSpec(links, delay, error)) =>
            Future {
              currentPending.incrementAndGet()
              maxSeen.updateAndGet(max => Math.max(max, currentPending.get()))
              delay.foreach {
                case fin: FiniteDuration  => Thread.sleep(fin.toMillis)
                case _: Duration.Infinite => Thread.sleep(Long.MaxValue)
              }
              visitedUris.getAndUpdate(v => v :+ path)
              currentPending.decrementAndGet()
              error.fold(Templating.page(links))(e => throw e)
            }

          case None => Future.failed(Exception(s"$trace: Page not found: $uri"))

      def store(key: String, value: String)(using trace: Trace): Future[Unit] =
        Future.unit

      def visited: Vector[String] = visitedUris.get()

      def maxParallelism: Int = maxSeen.get()

  def makeSyncServices: Fetch[Result] & Store[Result] & MockMetadata =
    new Fetch[Result] with Store[Result] with MockMetadata:
      val visitedUris = AtomicReference(Vector.empty[String])
      val maxSeen = AtomicInteger(0)
      val currentPending = AtomicInteger(0)

      def fetch(uri: Uri)(using trace: Trace): Result[String] =
        val path = normalisePath(uri)
        routes.get(path) match
          case Some(PageSpec(links, delay, error)) =>
            currentPending.incrementAndGet()
            maxSeen.updateAndGet(max => Math.max(max, currentPending.get()))
            delay.foreach {
              case fin: FiniteDuration  => Thread.sleep(fin.toMillis)
              case _: Duration.Infinite => Thread.sleep(Long.MaxValue)
            }
            visitedUris.getAndUpdate(v => v :+ path)
            currentPending.decrementAndGet()
            error.fold(Right(Templating.page(links)))(Left(_))

          case None => Left(Exception(s"$trace: Page not found: $uri"))

      def store(key: String, value: String)(using trace: Trace): Result[Unit] =
        Right(())

      def visited: Vector[String] = visitedUris.get()

      def maxParallelism: Int = maxSeen.get()

  def makeCatsServicesTF[F[_]: Async]: Fetch[F] & Store[F] & MockMetadata =
    new Fetch[F] with Store[F] with MockMetadata:
      val visitedUris = AtomicReference(Vector.empty[String])
      val maxSeen = AtomicInteger(0)
      val currentPending = AtomicInteger(0)

      def fetch(uri: Uri)(using trace: Trace): F[String] =
        val path = normalisePath(uri)
        routes.get(path) match
          case Some(PageSpec(links, maybeDelay, maybeError)) =>
            currentPending.incrementAndGet()
            maxSeen.updateAndGet(max => Math.max(max, currentPending.get()))
            for
              _ <- maybeDelay.fold(Sync[F].unit)(d => Async[F].sleep(d))
              _ <- Sync[F].delay(visitedUris.getAndUpdate(_ :+ path))
              result <- maybeError.fold(Sync[F].delay(Templating.page(links)))(e => Sync[F].raiseError(e))
              _ <- Sync[F].delay(currentPending.decrementAndGet())
            yield result
          case None =>
            Sync[F].raiseError(Exception(s"$trace: Page not found: $uri"))

      def store(key: String, value: String)(using trace: Trace): F[Unit] =
        Sync[F].unit

      def visited: Vector[String] = visitedUris.get()

      def maxParallelism: Int = maxSeen.get()

  def makeCatsServices: Fetch[IO] & Store[IO] & MockMetadata =
    new Fetch[IO] with Store[IO] with MockMetadata:
      val visitedUris = AtomicReference(Vector.empty[String])
      val maxSeen = AtomicInteger(0)
      val currentPending = AtomicInteger(0)

      def fetch(uri: Uri)(using trace: Trace): IO[String] =
        val path = normalisePath(uri)
        routes.get(path) match
          case Some(PageSpec(links, maybeDelay, maybeError)) =>
            currentPending.incrementAndGet()
            maxSeen.updateAndGet(max => Math.max(max, currentPending.get()))
            for
              _ <- maybeDelay.fold(IO.unit)(d => IO.sleep(d))
              _ <- IO(visitedUris.getAndUpdate(_ :+ path))
              result <- maybeError.fold(IO(Templating.page(links)))(e => IO.raiseError(e))
              _ <- IO(currentPending.decrementAndGet())
            yield result
          case None =>
            IO.raiseError(Exception(s"$trace: Page not found: $uri"))

      def store(key: String, value: String)(using trace: Trace): IO[Unit] =
        IO.unit

      def visited: Vector[String] = visitedUris.get()

      def maxParallelism: Int = maxSeen.get()

  def makeZioServices: Fetch[ZTask] & Store[ZTask] & MockMetadata =
    new Fetch[ZTask] with Store[ZTask] with MockMetadata:
      val visitedUris = AtomicReference(Vector.empty[String])
      val maxSeen = AtomicInteger(0)
      val currentPending = AtomicInteger(0)

      def fetch(uri: Uri)(using trace: Trace): ZTask[String] =
        val path = normalisePath(uri)
        routes.get(path) match
          case Some(PageSpec(links, maybeDelay, maybeError)) =>
            currentPending.incrementAndGet()
            maxSeen.updateAndGet(max => Math.max(max, currentPending.get()))
            for
              _ <- maybeDelay.fold(ZIO.unit)(d => ZIO.sleep(zio.Duration.fromScala(d)))
              _ <- ZIO.succeed(visitedUris.getAndUpdate(_ :+ path))
              result <- maybeError.fold(ZIO.succeed(Templating.page(links)))(e => ZIO.fail(e))
              _ <- ZIO.succeed(currentPending.decrementAndGet())
            yield result
          case None =>
            ZIO.fail(Exception(s"$trace: Page not found: $uri"))

      def store(key: String, value: String)(using trace: Trace): ZTask[Unit] =
        ZIO.unit

      def visited: Vector[String] = visitedUris.get()

      def maxParallelism: Int = maxSeen.get()

  def makeKyoServices: Fetch[Kyo] & Store[Kyo] & MockMetadata =
    new Fetch[Kyo] with Store[Kyo] with MockMetadata:
      val visitedUris = AtomicReference(Vector.empty[String])
      val maxSeen = AtomicInteger(0)
      val currentPending = AtomicInteger(0)

      def fetch(uri: Uri)(using trace: Trace): Kyo[String] =
        val path = normalisePath(uri)
        routes.get(path) match
          case Some(PageSpec(links, maybeDelay, maybeError)) =>
            currentPending.incrementAndGet()
            maxSeen.updateAndGet(max => Math.max(max, currentPending.get()))
            for
              _ <- maybeDelay.fold(Kyo.unit)(d => Kyo.sleep(kyo.Duration.fromScala(d)))
              _ <- KyoSync.defer(visitedUris.getAndUpdate(_ :+ path))
              result <- maybeError.fold(KyoSync.defer(Templating.page(links)))(e => Kyo.fail(e))
              _ <- KyoSync.defer(currentPending.decrementAndGet())
            yield result
          case None =>
            Kyo.fail(Exception(s"$trace: Page not found: $uri"))

      def store(key: String, value: String)(using trace: Trace): Kyo[Unit] =
        Kyo.unit

      def visited: Vector[String] = visitedUris.get()

      def maxParallelism: Int = maxSeen.get()

  Map(
    "future" -> makeFutureScraper,
    "cats" -> makeCatsScraper,
    "zio" -> makeZioScraper,
    "kyo" -> makeKyoScraper,
    "ox" -> makeOxScraper,
    "gears" -> makeGearsScraper,
    "cats-tf" -> makeCatsScraperTF,
    "future-high-level" -> makeFutureScraperHighLevel,
    "cats-high-level" -> makeCatsScraperHighLevel,
    "zio-high-level" -> makeZioScraperHighLevel,
    "kyo-high-level" -> makeKyoScraperHighLevel,
    "ox-high-level" -> makeOxScraperHighLevel,
    "gears-high-level" -> makeGearsScraperHighLevel,
    "cats-high-tf" -> makeCatsScraperHighTF
  )
end makeScrapers
