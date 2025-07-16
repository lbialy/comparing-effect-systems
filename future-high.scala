package ma.chinespirit.crawldown

import scala.concurrent.*
import scala.concurrent.duration.Duration
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.*
import sttp.model.Uri
import scala.util.*

class FutureScraperHighLevel(
    fetch: Fetch[Future],
    store: Store[Future],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
)(using ec: ExecutionContext):

  private val queue = LinkedBlockingQueue[Scrape | Done]()
  private val inFlight = AtomicInteger(0)
  private val visited = AtomicReference[Set[Uri]](Set.empty)

  def start(): Unit =
    queue.put(Scrape(root, 0))
    inFlight.incrementAndGet()

    Await.result(Future.sequence(Vector.fill(parallelism)(worker(queue, inFlight))), Duration.Inf)

    println("FutureScraperHighLevel: Finished.")

  private def worker(queue: LinkedBlockingQueue[Scrape | Done], inFlight: AtomicInteger): Future[Unit] =
    Future { blocking(queue.take()) }.flatMap {
      case Scrape(uri, depth) =>
        given trace: Trace = Trace(uri)
        val result = if depth >= maxDepth then
          println(s"$trace: FutureScraperHighLevel: depth limit – skipping $uri")
          Future.unit
        else if visited.getAndUpdate(_ + uri).contains(uri) then
          println(s"$trace: FutureScraperHighLevel: skipping $uri")
          Future.unit
        else crawl(uri, depth)

        result.transformWith {
          case Success(_) =>
            val currentInFlight = inFlight.decrementAndGet()
            if currentInFlight > 0 then worker(queue, inFlight)
            else Future(Vector.fill(parallelism)(Done).foreach(queue.put))
          case Failure(ex) =>
            Future.failed(ex)
        }

      case Done =>
        println(s"FutureScraperHighLevel: Received poison pill.")
        Future.unit
    }

  private def crawl(uri: Uri, depth: Int)(using trace: Trace): Future[Unit] =
    for
      _ <- Future { println(s"$trace: FutureScraperHighLevel: crawling $uri") }
      content <- fetch.fetch(uri)
      (links, markdown) <- MdConverter.convertAndExtractLinks(content, uri, selector).toFuture
      pushFrontier = Future {
        links
          .map(Scrape(_, depth + 1))
          .foreach(scrape => {
            inFlight.incrementAndGet()
            queue.put(scrape)
          })
      }
      persist = store.store(Names.toFilename(uri, root), markdown)
      _ <- Future.sequence(List(pushFrontier, persist))
    yield ()
