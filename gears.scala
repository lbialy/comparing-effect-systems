package ma.chinespirit.crawldown

import scala.language.experimental.captureChecking

import java.util.concurrent.{ArrayBlockingQueue, Semaphore}
import sttp.model.Uri
import gears.async.{Future => _, *}
import gears.async.default.given
import steps.result.*, Result.eval.*
import scala.annotation.tailrec

class GearsScraper(
    fetch: Fetch[ResultEx],
    store: Store[ResultEx],
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

  @tailrec
  private def coordinator(visited: Set[Uri], inFlight: Int)(using Async.Spawn): Unit =
    if queue.isEmpty && inFlight == 0 then ()
    else
      queue.take() match
        case ex: Throwable => throw ex
        case Scrape(uri, depth) =>
          if depth >= maxDepth then coordinator(visited, inFlight)
          else if !visited.contains(uri) then
            Future:
              crawl(uri, depth) match
                case Result.Err(ex) => queue.put(ex)
                case Result.Ok(_)   => ()

            coordinator(visited + uri, inFlight + 1)
          else coordinator(visited, inFlight)
        case Done =>
          coordinator(visited, inFlight - 1)

  private def crawl(uri: Uri, depth: Int)(using Async.Spawn): ResultEx[Unit] =
    JvmAsyncOperations.jvmInterruptible:
      Result:
        semaphore.acquire()
        try
          val content = fetch.fetch(uri).ok
          val (links, markdown) = MdConverter.convertAndExtractLinks(content, uri, selector).toResult.ok

          val key = Names.toFilename(uri, root)
          def pushFrontier = links.map(Scrape(_, depth + 1)).foreach(queue.put)
          def persist = store.store(key, markdown)

          Async.group:
            val (_, persistResult) = Future(pushFrontier).zip(Future(persist)).await
            persistResult.ok
        finally
          semaphore.release()
          queue.put(Done)
