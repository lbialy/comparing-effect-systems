package ma.chinespirit.crawldown

import java.util.concurrent.{ArrayBlockingQueue, Semaphore}
import sttp.model.Uri
import ox.*, either.*
import scala.annotation.tailrec

class OxScraper(
    fetch: Fetch[Result],
    store: Store[Result],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8,
    queueCapacity: Int = 10
):
  private val queue = ArrayBlockingQueue[Scrape | Done](queueCapacity)
  private val semaphore = Semaphore(parallelism)

  def start(): Unit =
    queue.put(Scrape(root, 0))

    supervised:
      coordinator(Set.empty, 0)

  end start

  @tailrec
  private def coordinator(visited: Set[Uri], inFlight: Int)(using Ox): Unit =
    if queue.isEmpty && inFlight == 0 then ()
    else
      queue.take() match
        case Scrape(uri, depth) =>
          if depth >= maxDepth then coordinator(visited, inFlight)
          else if !visited.contains(uri) then
            forkUser:
              crawl(uri, depth).orThrow
            coordinator(visited + uri, inFlight + 1)
          else coordinator(visited, inFlight)
        case Done =>
          coordinator(visited, inFlight - 1)

  private def crawl(uri: Uri, depth: Int): Result[Unit] = either:
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
