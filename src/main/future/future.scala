package ma.chinespirit.crawldown

import java.util.concurrent.*
import scala.concurrent.{Future, ExecutionContext, blocking}
import sttp.model.Uri
import scala.util.*

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

  type Queue = ArrayBlockingQueue[Scrape | Done | Throwable]

  def start(): Unit =
    val queue = ArrayBlockingQueue[Scrape | Done | Throwable](queueCapacity)
    val semaphore = Semaphore(parallelism)
    queue.put(Scrape(root, 0))
    val visited = Set.empty[Uri]
    coordinator(visited, 0, queue, semaphore)

  private def coordinator(visited: Set[Uri], inFlight: Int, queue: Queue, semaphore: Semaphore): Unit =
    if queue.isEmpty && inFlight == 0 then ()
    else
      queue.take() match
        case ex: Throwable => throw ex
        case Scrape(url, depth) =>
          if depth >= maxDepth then coordinator(visited, inFlight, queue, semaphore)
          else if !visited.contains(url) then
            crawl(url, depth, queue, semaphore)
            coordinator(visited + url, inFlight + 1, queue, semaphore)
          else coordinator(visited, inFlight, queue, semaphore)

        case Done =>
          coordinator(visited, inFlight - 1, queue, semaphore)

  private def crawl(uri: Uri, depth: Int, queue: Queue, semaphore: Semaphore): Future[Unit] =
    val result = for
      _ <- Future { blocking { semaphore.acquire() } }
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
