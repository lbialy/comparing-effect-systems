package ma.chinespirit.crawldown

import sttp.model.Uri
import gears.async.*
import gears.async.default.given
import scala.annotation.tailrec
import scala.util.boundary, boundary.break
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.atomic.*

class GearsScraperHighLevel(
    fetch: Fetch[Result],
    store: Store[Result],
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

    println("GearsScraperHighLevel: Finished.")

  @tailrec
  private def worker(queue: LinkedBlockingDeque[Scrape | Done], inFlight: AtomicInteger)(using Async): Unit =
    queue.take() match
      case Done => println("GearsScraperHighLevel: Received poison pill.")
      case Scrape(uri, depth) =>
        given trace: Trace = Trace(uri)
        if depth >= maxDepth then println(s"$trace: GearsScraperHighLevel: depth limit – skipping $uri")
        else if visited.getAndUpdate(_ + uri).contains(uri) then println(s"$trace: GearsScraperHighLevel: skipping $uri")
        else
          crawl(uri, depth) match
            case Left(e)  => throw e
            case Right(_) => ()

        val currentInFlight = inFlight.decrementAndGet()
        if currentInFlight > 0 then worker(queue, inFlight)
        else for _ <- 0 until parallelism do queue.put(Done)

  private def crawl(uri: Uri, depth: Int)(using trace: Trace, async: Async): Result[Unit] =
    JvmAsyncOperations.jvmInterruptible:
      boundary[Result[Unit]]:
        val content = fetch.fetch(uri) match
          case Left(e)        => break(Left(e))
          case Right(content) => content

        val (links, markdown) = MdConverter.convertAndExtractLinks(content, uri, selector) match
          case Left(e)                  => break(Left(e))
          case Right((links, markdown)) => (links, markdown)

        val key = Names.toFilename(uri, root)
        def pushFrontier = links.map(Scrape(_, depth + 1)).foreach { link =>
          inFlight.incrementAndGet()
          queue.put(link)
        }
        def persist = store.store(key, markdown) match
          case Left(e)  => break(Left(e))
          case Right(_) => ()

        Async.group:
          Future(pushFrontier).zip(Future(persist)).await

        Right(())
