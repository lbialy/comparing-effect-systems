package ma.chinespirit.crawldown

import zio.{Task => ZTask, Trace => ZTrace, *}
import sttp.model.Uri

class ZIOScraper(
    fetch: Fetch[ZTask],
    store: Store[ZTask],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8,
    queueCapacity: Int = 10
):
  def start: ZTask[Unit] =
    for
      queue <- Queue.bounded[Scrape | Done | Throwable](queueCapacity)
      _ <- queue.offer(Scrape(root, 0))
      sem <- Semaphore.make(parallelism.toLong)
      visitedRef <- Ref.make(Set.empty[Uri])
      inFlightRef <- Ref.make(0)
      _ <- ZIO.scoped {
        def coordinator: ZIO[Scope, Throwable, Unit] = {
          ZIO.ifZIO {
            queue.isEmpty
              .zip(inFlightRef.get)
              .map((empty, inFlight) => empty && inFlight == 0)
          }(
            onTrue = ZIO.unit,
            onFalse = queue.take.flatMap {
              case ex: Throwable => ZIO.fail(ex)
              case Scrape(uri, depth) =>
                given trace: Trace = Trace(uri)
                for
                  seen <- visitedRef.get.map(_.contains(uri))
                  _ <-
                    if depth >= maxDepth then ZIO.logInfo(s"[$trace]: depth limit — skipping $uri")
                    else if seen then ZIO.logInfo(s"[$trace]: already visited — skipping $uri")
                    else
                      for
                        _ <- visitedRef.update(_ + uri)
                        _ <- inFlightRef.update(_ + 1)
                        _ <- ZIO.logInfo(s"[$trace]: ZIOScraper: crawling $uri")
                        _ <- crawl(uri, depth, queue, sem).forkScoped
                      yield ()
                  _ <- coordinator
                yield ()
              case Done => inFlightRef.update(_ - 1) *> coordinator
            }
          )
        }

        coordinator
      }
      _ <- ZIO.logInfo("ZIOScraper: Finished.")
    yield ()

  private def crawl(
      uri: Uri,
      depth: Int,
      queue: Queue[Scrape | Done | Throwable],
      semaphore: Semaphore
  )(using trace: Trace): UIO[Unit] =
    semaphore
      .withPermit {
        for
          content <- fetch.fetch(uri)
          (links, markdown) <- ZIO.fromEither(MdConverter.convertAndExtractLinks(content, uri, selector))
          pushFrontier = ZIO.foreachDiscard(links)(l => queue.offer(Scrape(l, depth + 1)))
          persist = store.store(Names.toFilename(uri, root), markdown)
          _ <- pushFrontier.zipPar(persist)
        yield ()
      }
      .catchAll { case ex => queue.offer(ex).unit }
      .ensuring(queue.offer(Done).unit)
      .onInterrupt(ZIO.logInfo(s"[$trace]: ZIOScraper: cancelled $uri"))
