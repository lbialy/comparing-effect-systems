package ma.chinespirit.crawldown

import scala.language.experimental.captureChecking

import sttp.model.Uri
import gears.async.{Future => _, *}
import gears.async.default.given
import scala.annotation.tailrec
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.atomic.*
import steps.result.*, Result.eval.*

class GearsScraperHighLevel(
    fetch: Fetch[ResultEx],
    store: Store[ResultEx],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
):
  private val queue = LinkedBlockingDeque[Scrape | Done]()
  private val inFlight = AtomicInteger(0)
  private val visited = AtomicReference[Set[Uri]](Set.empty)

  def start(): Unit =
    inFlight.incrementAndGet()
    queue.put(Scrape(root, 0))

    Async.blocking:
      Vector.fill(parallelism)(Future(worker(queue, inFlight))).awaitAllOrCancel

  @tailrec
  private def worker(queue: LinkedBlockingDeque[Scrape | Done], inFlight: AtomicInteger)(using Async.Spawn): Unit =
    queue.take() match
      case Done => ()
      case Scrape(uri, depth) =>
        if depth >= maxDepth then ()
        else if visited.getAndUpdate(_ + uri).contains(uri) then ()
        else
          crawl(uri, depth) match
            case Result.Err(e) => throw e
            case Result.Ok(_)  => ()

        val currentInFlight = inFlight.decrementAndGet()
        if currentInFlight > 0 then worker(queue, inFlight)
        else for _ <- 0 until parallelism do queue.put(Done)

  private def crawl(uri: Uri, depth: Int)(using async: Async.Spawn): ResultEx[Unit] =
    JvmAsyncOperations.jvmInterruptible:
      Result:
        val content = fetch.fetch(uri).ok
        val (links, markdown) = MdConverter.convertAndExtractLinks(content, uri, selector).toResult.ok

        val key = Names.toFilename(uri, root)
        def pushFrontier = links.map(Scrape(_, depth + 1)).foreach { link =>
          inFlight.incrementAndGet()
          queue.put(link)
        }
        def persist = store.store(key, markdown)

        Async.group:
          val (_, persistResult) = Future(pushFrontier).zip(Future(persist)).await
          persistResult.ok
