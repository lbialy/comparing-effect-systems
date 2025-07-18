package ma.chinespirit.crawldown

import sttp.model.Uri
import kyo.*

class KyoScraperHighLevel(
    fetch: Fetch[Kyo],
    store: Store[Kyo],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
):
  def start: Unit < (Async & Abort[Throwable]) =
    Scope.run:
      for
        queue <- Channel.init[Scrape](Int.MaxValue)
        inFlight <- AtomicInt.init(0)
        visited <- AtomicRef.init(Set.empty[Uri])
        _ <- inFlight.incrementAndGet
        _ <- queue.put(Scrape(root, 0))
        _ <- Async.fill(parallelism, parallelism)(worker(queue, inFlight, visited))
      yield ()

  private def worker(
      queue: Channel[Scrape],
      inFlight: AtomicInt,
      visited: AtomicRef[Set[Uri]]
  ): Unit < (Async & Abort[Throwable]) =
    Loop.foreach:
      Abort.recover[Closed](_ => Loop.done):
        queue.take.map:
          case Scrape(uri, depth) =>
            val processUri =
              if depth >= maxDepth then Kyo.unit
              else
                visited
                  .getAndUpdate(_ + uri)
                  .map: visitedSet =>
                    if visitedSet.contains(uri) then Kyo.unit
                    else crawl(uri, depth, inFlight, queue)

            processUri.andThen:
              inFlight.decrementAndGet.map: currentInFlight =>
                if currentInFlight > 0 then Loop.continue
                else queue.closeAwaitEmpty.andThen(Loop.done)

  def crawl(uri: Uri, depth: Int, inFlight: AtomicInt, queue: Channel[Scrape]): Unit < (Async & Abort[Throwable]) =
    for
      content <- fetch.fetch(uri)
      (links, markdown) <- Abort.get(MdConverter.convertAndExtractLinks(content, uri, selector))
      pushFrontier = inFlight.addAndGet(links.size).andThen(queue.putBatch(links.map(Scrape(_, depth + 1))))
      persist = store.store(Names.toFilename(uri, root), markdown)
      _ <- Async.zip(pushFrontier, persist)
    yield ()
