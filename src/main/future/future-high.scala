package ma.chinespirit.crawldown

import scala.concurrent.*
import scala.concurrent.duration.Duration
import java.util.concurrent.PriorityBlockingQueue
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

  type Queue = PriorityBlockingQueue[Scrape | Done]
  type Visited = AtomicReference[Set[Uri]]

  def start(): Unit =
    val initialCapacity = 11
    val queue = PriorityBlockingQueue[Scrape | Done](
      initialCapacity,
      Ordering.fromLessThan {
        case (Done, _: Scrape) => true
        case _                 => false
      }
    )
    val inFlight = AtomicInteger(0)
    val visited = AtomicReference[Set[Uri]](Set.empty)

    queue.put(Scrape(root, 0))

    inFlight.incrementAndGet()

    Await.result(Future.sequence(Vector.fill(parallelism)(worker(queue, inFlight, visited))), Duration.Inf)

  private def worker(queue: Queue, inFlight: AtomicInteger, visited: Visited): Future[Unit] =
    Future { blocking(queue.take()) }.flatMap {
      case Scrape(uri, depth) =>
        val result =
          if depth >= maxDepth then Future.unit
          else if visited.getAndUpdate(_ + uri).contains(uri) then Future.unit
          else crawl(uri, depth, queue, inFlight)

        result.transformWith {
          case Success(_) =>
            val currentInFlight = inFlight.decrementAndGet()
            if currentInFlight > 0 then worker(queue, inFlight, visited)
            else Future(Vector.fill(parallelism)(Done).foreach(queue.put))
          case Failure(ex) =>
            Future(Vector.fill(parallelism)(Done).foreach(queue.put)).flatMap(_ => Future.failed(ex))
        }

      case Done =>
        Future.unit
    }

  private def crawl(uri: Uri, depth: Int, queue: Queue, inFlight: AtomicInteger): Future[Unit] =
    for
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
