package ma.chinespirit.crawldown

import java.util.concurrent.*
import scala.concurrent.{Future, ExecutionContext}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import sttp.model.Uri
import scala.util.*
import java.util.concurrent.atomic.AtomicBoolean

extension [A](either: => Either[Throwable, A])
  def toFuture: Future[A] =
    Future.fromTry(either.toTry)

class FutureScraper(
    fetch: Fetch[Future],
    store: Store[Future],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8,
    queueCapacity: Int = 10
)(using ec: ExecutionContext):

  private val queue = ArrayBlockingQueue[Scrape | Done | Throwable](queueCapacity)
  private val semaphore = Semaphore(parallelism)

  def start(): Unit =
    queue.put(Scrape(root, 0))
    val visited = Set.empty[Uri]
    coordinator(visited, 0)

  private def coordinator(visited: Set[Uri], inFlight: Int): Unit =
    if queue.isEmpty && inFlight == 0 then ()
    else
      queue.take() match
        case ex: Throwable => throw ex
        case Scrape(url, depth) =>
          if depth >= maxDepth then coordinator(visited, inFlight)
          else if !visited.contains(url) then
            crawl(url, depth)
            coordinator(visited + url, inFlight + 1)
          else coordinator(visited, inFlight)

        case Done =>
          coordinator(visited, inFlight - 1)

  private def crawl(uri: Uri, depth: Int): Future[Unit] =
    val result = for
      _ <- Future { semaphore.acquire() }
      content <- fetch.fetch(uri)
      (links, markdown) <- MdConverter.convertAndExtractLinks(content, uri, selector).toFuture
      pushFrontier = Future { links.map(Scrape(_, depth + 1)).foreach(queue.put) }
      persist = store.store(Names.toFilename(uri, root), markdown)
      _ <- Future.sequence(List(pushFrontier, persist))
    yield ()

    result.onComplete {
      case Failure(exception) =>
        semaphore.release()
        queue.put(exception)
        queue.put(Done)
      case Success(_) =>
        semaphore.release()
        queue.put(Done)
    }

    result
  end crawl
