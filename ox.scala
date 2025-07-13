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
      coordinator(Set.empty, 0L, 0L)

    println("OxScraper: Finished.")
  end start

  @tailrec
  private def coordinator(visited: Set[Uri], spawned: Long, done: Long)(using Ox): Unit =
    if queue.isEmpty && spawned == done then ()
    else
      queue.take() match
        case Scrape(uri, depth) =>
          given trace: Trace = Trace(uri)
          if depth >= maxDepth then
            println(s"$trace: Skipping $uri because max depth was reached")
            coordinator(visited, spawned, done)
          else if !visited.contains(uri) then
            println(s"$trace: OxScraper: crawling $uri")
            forkUser:
              crawl(uri, depth).orThrow
            coordinator(visited + uri, spawned + 1, done)
          else
            println(s"$trace: Skipping $uri because it has already been visited")
            coordinator(visited, spawned, done)
        case Done =>
          coordinator(visited, spawned, done + 1)

  private def crawl(uri: Uri, depth: Int)(using trace: Trace): Result[Unit] = either:
    semaphore.acquire()
    try
      val content = fetch.fetch(uri).ok()
      val (links, markdown) = MdConverter.convertAndExtractLinks(content, uri, selector).ok()
      val key = Names.toFilename(uri, root)
      def pushFrontier = links.map(Scrape(_, depth + 1)).foreach(queue.put)
      def persist = store.store(key, markdown).ok()
      par(pushFrontier, persist)
    catch
      case iex: InterruptedException =>
        println(s"$trace: OxScraper: interrupted $uri")
        throw iex
    finally
      semaphore.release()
      queue.put(Done)
