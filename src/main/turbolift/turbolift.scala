package ma.chinespirit.crawldown

import turbolift.effects.IO
import turbolift.io.{Channel, Semaphore, Warp}
import turbolift.!!
import turbolift.bindless._
import turbolift.Extensions._
import sttp.model.Uri

@annotation.nowarn("msg=unused (explicit parameter|pattern variable)") // for `do` macro
final class TurboliftScraper(
    fetch: Fetch[TTask],
    store: Store[TTask],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8,
    queueCapacity: Int = 10
):
  def start: Unit !! IO =
    `do`:
      val queue = Channel.bounded[Scrape | Done | Throwable](queueCapacity).!
      val semaphore = Semaphore.create(parallelism).!
      queue.put(Scrape(root, 0)).!
      coordinator(Set.empty, queue, semaphore, 0).!
    .warp

  private def coordinator(
      visited: Set[Uri],
      queue: Channel[Scrape | Done | Throwable],
      semaphore: Semaphore,
      inFlight: Int
  ): Unit !! (IO & Warp) =
    def loop = coordinator(_, queue, semaphore, _)
    `do`:
      if queue.isEmpty.! && inFlight == 0 then ()
      else
        queue.get.! match
          case Scrape(uri, depth) =>
            if depth >= maxDepth || visited.contains(uri) then loop(visited, inFlight).!
            else
              val _ = crawl(uri, depth, queue, semaphore).fork.!
              loop(visited + uri, inFlight + 1).!
          case Done          => loop(visited, inFlight - 1).!
          case ex: Throwable => IO.raise(ex).!

  private def crawl(uri: Uri, depth: Int, queue: Channel[Scrape | Done | Throwable], semaphore: Semaphore): Unit !! IO =
    IO.catchAllEff:
      semaphore.use(1):
        `do`:
          val content = fetch.fetch(uri).!
          val (links, markdown) = IO.raiseFromEither(MdConverter.convertAndExtractLinks(content, uri, selector)).!
          val pushFrontier = links.foreachEff(uri => queue.put(Scrape(uri, depth + 1)))
          val persist = store.store(Names.toFilename(uri, root), markdown)
          (pushFrontier &! persist).!
    .apply(ex => queue.put(ex))
      .guarantee(queue.put(Done))
