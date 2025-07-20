package ma.chinespirit.crawldown

import sttp.model.Uri
import kyo.*

extension [S](self: Boolean < S)
  def ifEff[A](`then`: => A < S, `else`: => A < S): A < S =
    self.map(if _ then `then` else `else`)

class KyoScraper(
    fetch: Fetch[Kyo],
    store: Store[Kyo],
    tracing: TracingKyo,
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8,
    queueCapacity: Int = 10
):
  def start: Unit < (Async & Abort[Throwable]) =
    tracing.root("start"):
      Scope.run: // scope for Channel
        for
          queue <- Channel.init[Scrape | Done | Throwable](queueCapacity, access = Access.MultiProducerSingleConsumer)
          semaphore <- Meter.initSemaphore(parallelism)
          visited <- AtomicRef.init[Set[Uri]](Set.empty)
          inFlight <- AtomicRef.init(0)
          _ <- queue.put(Scrape(root, 0))
          _ <-
            Scope.run: // Scope for forked worker fibers
              Loop.foreach:
                (queue.empty <*> inFlight.get)
                  .map { case (empty, inFlight) => empty && inFlight == 0 }
                  .ifEff(
                    Loop.done,
                    queue.take.map:
                      case ex: Throwable => Abort.fail(ex)
                      case Scrape(uri, depth) =>
                        for
                          seen <- visited.get.map(_.contains(uri))
                          _ <-
                            if depth >= maxDepth then Kyo.unit
                            else if seen then Kyo.unit
                            else
                              for
                                _ <- visited.updateAndGet(_ + uri)
                                _ <- inFlight.updateAndGet(_ + 1)
                                _ <- crawl(uri, depth, queue, semaphore).forkScoped
                              yield ()
                        yield Loop.continue
                      case Done =>
                        inFlight.updateAndGet(_ - 1) *> Loop.continue
                  )
        yield ()

  private def crawl(uri: Uri, depth: Int, queue: Channel[Scrape | Done | Throwable], semaphore: Meter)(using
      Trace
  ): Unit < (Async & Abort[Throwable]) =
    tracing.span(s"crawl(${uri.pathToString})"):
      Abort.recover[Closed](_ => ()):
        semaphore.run:
          Scope.run:
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
