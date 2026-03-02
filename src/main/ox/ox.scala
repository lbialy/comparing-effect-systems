package ma.chinespirit.crawldown

import java.util.concurrent.{ArrayBlockingQueue, Semaphore}
import sttp.model.Uri
import ox.*, either.*
import scala.annotation.tailrec

class OxScraper(
    fetch: Fetch[EitherErrorOr],
    store: Store[EitherErrorOr],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8,
    queueCapacity: Int = 10
):
  type Queue = ArrayBlockingQueue[Scrape | Done]

  def start(): Unit =
    val queue = ArrayBlockingQueue[Scrape | Done](queueCapacity)
    val semaphore = Semaphore(parallelism)

    queue.put(Scrape(root, 0))

    supervised:
      coordinator(Set.empty, queue, semaphore, 0)

  @tailrec
  private def coordinator(visited: Set[Uri], queue: Queue, semaphore: Semaphore, inFlight: Int)(using Ox): Unit =
    if queue.isEmpty && inFlight == 0 then ()
    else
      queue.take() match
        case Scrape(uri, depth) =>
          if depth >= maxDepth then coordinator(visited, queue, semaphore, inFlight)
          else if !visited.contains(uri) then
            forkUser:
              crawl(uri, depth, queue, semaphore).orThrow
            coordinator(visited + uri, queue, semaphore, inFlight + 1)
          else coordinator(visited, queue, semaphore, inFlight)
        case Done =>
          coordinator(visited, queue, semaphore, inFlight - 1)

  private def crawl(uri: Uri, depth: Int, queue: Queue, semaphore: Semaphore): Either[Throwable, Unit] =
    either:
      semaphore.acquire()
      try
        val content = fetch.fetch(uri).ok()
        val (links, markdown) = MdConverter.convertAndExtractLinks(content, uri, selector).ok()
        val key = Names.toFilename(uri, root)
        def pushFrontier = links.map(Scrape(_, depth + 1)).foreach(queue.put)
        def persist = store.store(key, markdown).ok()
        par(pushFrontier, persist)
      finally
        semaphore.release()
        queue.put(Done)
