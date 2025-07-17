package ma.chinespirit.crawldown

import java.util.concurrent.{ArrayBlockingQueue, Semaphore}
import sttp.model.Uri
import gears.async.*
import gears.async.default.given
import scala.annotation.tailrec
import scala.util.boundary, boundary.break

class GearsScraper(
    fetch: Fetch[Result],
    store: Store[Result],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8,
    queueCapacity: Int = 10
):
  private val queue = ArrayBlockingQueue[Scrape | Done | Throwable](queueCapacity)
  private val semaphore = Semaphore(parallelism)

  def start(): Unit =
    queue.put(Scrape(root, 0))

    Async.blocking:
      coordinator(Set.empty, 0)

    println("GearsScraper: Finished.")

  @tailrec
  private def coordinator(visited: Set[Uri], inFlight: Int)(using Async.Spawn): Unit =
    if queue.isEmpty && inFlight == 0 then ()
    else
      queue.take() match
        case ex: Throwable => throw ex
        case Scrape(uri, depth) =>
          given trace: Trace = Trace(uri)
          if depth >= maxDepth then
            println(s"$trace: GearsScraper: Skipping $uri because max depth was reached")
            coordinator(visited, inFlight)
          else if !visited.contains(uri) then
            println(s"$trace: GearsScraper: crawling $uri")
            Future:
              crawl(uri, depth) match
                case Left(ex) => queue.put(ex)
                case Right(_) => ()

            coordinator(visited + uri, inFlight + 1)
          else
            println(s"$trace: GearsScraper: Skipping $uri because it has already been visited")
            coordinator(visited, inFlight)
        case Done =>
          coordinator(visited, inFlight - 1)

  private def crawl(uri: Uri, depth: Int)(using trace: Trace, async: Async): Result[Unit] =
    JvmAsyncOperations.jvmInterruptible:
      boundary[Result[Unit]]:
        semaphore.acquire()
        try
          val content = fetch.fetch(uri) match
            case Left(e)        => break(Left(e))
            case Right(content) => content

          val (links, markdown) = MdConverter.convertAndExtractLinks(content, uri, selector) match
            case Left(e)                  => break(Left(e))
            case Right((links, markdown)) => (links, markdown)

          val key = Names.toFilename(uri, root)
          def pushFrontier = links.map(Scrape(_, depth + 1)).foreach(queue.put)
          def persist = store.store(key, markdown) match
            case Left(e)  => break(Left(e))
            case Right(_) => ()

          Async.group:
            Future(pushFrontier).zip(Future(persist)).await

          Right(())
        catch
          case iex: InterruptedException =>
            println(s"$trace: GearsScraper: interrupted $uri")
            throw iex
        finally
          semaphore.release()
          queue.put(Done)
