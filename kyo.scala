package ma.chinespirit.crawldown

import sttp.model.Uri
import kyo.*

extension [S](self: Boolean < S)
  def ifEff[A](`then`: => A < S, `else`: => A < S): A < S =
    self.map(if _ then `then` else `else`)

class KyoScraper(
    fetch: Fetch[Kyo],
    store: Store[Kyo],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8,
    queueCapacity: Int = 10
):
  def start: Unit < (Async & Abort[Throwable]) = Scope.run:
    for
      queue <- Channel.init[Scrape | Done | Throwable](queueCapacity, access = Access.MultiProducerSingleConsumer)
      semaphore <- Meter.initSemaphore(parallelism)
      visited <- AtomicRef.init[Set[Uri]](Set.empty)
      inFlight <- AtomicRef.init(0)
      _ <- queue.put(Scrape(root, 0))
      _ <- Scope.run:
        Loop.foreach:
          (queue.empty <*> inFlight.get)
            .map { case (empty, inFlight) => empty && inFlight == 0 }
            .ifEff(
              Loop.done,
              queue.take.map:
                case ex: Throwable => Kyo.fail(ex)
                case Scrape(uri, depth) =>
                  given trace: Trace = Trace(uri)
                  for
                    seen <- visited.get.map(_.contains(uri))
                    _ <-
                      if depth >= maxDepth then Kyo.logInfo(s"$trace: depth limit – skipping $uri")
                      else if seen then Kyo.logInfo(s"$trace: already visited – skipping $uri")
                      else
                        for
                          _ <- visited.updateAndGet(_ + uri)
                          _ <- inFlight.updateAndGet(_ + 1)
                          _ <- Kyo.logInfo(s"$trace: KyoScraper: crawling $uri")
                          fiber <- crawl(uri, depth, queue, semaphore).forkScoped
                          _ <- fiber.onInterrupt(_ => Kyo.logInfo(s"$trace: KyoScraper: cancelled $uri"))
                        yield ()
                  yield Loop.continue
                case Done =>
                  inFlight.updateAndGet(_ - 1) *> Loop.continue
            )
      _ <- Kyo.logInfo("KyoScraper: Finished.")
    yield ()

  private def crawl(uri: Uri, depth: Int, queue: Channel[Scrape | Done | Throwable], semaphore: Meter)(using
      trace: Trace
  ): Unit < (Async & Abort[Throwable]) =
    semaphore.run:
      Kyo.scoped:
        Resource
          .ensure(queue.put(Done))
          .andThen:
            for
              content <- fetch.fetch(uri)
              (links, markdown) <- Abort.get(MdConverter.convertAndExtractLinks(content, uri, selector))
              pushFrontier = Kyo.collectAllDiscard(links.map(url => queue.put(Scrape(url, depth + 1))))
              persist = store.store(Names.toFilename(uri, root), markdown)
              _ <- Async.zip(pushFrontier, persist)
            yield ()
          .recover:
            case ex => queue.put(ex)
