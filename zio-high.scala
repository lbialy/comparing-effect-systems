package ma.chinespirit.crawldown

import zio.{Task => ZTask, Trace => ZTrace, *}
import sttp.model.Uri

class ZIOScraperHighLevel(
    fetch: Fetch[ZTask],
    store: Store[ZTask],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
):
  def start: ZTask[Unit] =
    for
      queue <- Queue.unbounded[Scrape | Done]
      visitedRef <- Ref.make(Set.empty[Uri])
      inFlight <- Ref.make(0)
      _ <- inFlight.update(_ + 1)
      _ <- queue.offer(Scrape(root, 0))
      _ <- ZIO.collectAllParDiscard(Vector.fill(parallelism)(worker(queue, visitedRef, inFlight)))
    yield ()

  private def worker(queue: Queue[Scrape | Done], visitedRef: Ref[Set[Uri]], inFlight: Ref[Int]): ZTask[Unit] =
    queue.take.flatMap {
      case Done => ZIO.unit
      case Scrape(uri, depth) =>
        val handleUri =
          if depth >= maxDepth then ZIO.unit
          else
            visitedRef.getAndUpdate(_ + uri).flatMap { visitedSet =>
              if visitedSet.contains(uri) then ZIO.unit
              else crawl(uri, depth, queue, inFlight)
            }

        handleUri *> inFlight.updateAndGet(_ - 1).flatMap { currentInFlight =>
          if currentInFlight > 0 then worker(queue, visitedRef, inFlight)
          else queue.offerAll(Vector.fill(parallelism)(Done)).unit
        }
    }

  private def crawl(uri: Uri, depth: Int, queue: Queue[Scrape | Done], inFlight: Ref[Int]): ZTask[Unit] =
    for
      content <- fetch.fetch(uri)
      (links, markdown) <- ZIO.fromEither(MdConverter.convertAndExtractLinks(content, uri, selector))
      pushFrontier = ZIO.foreachDiscard(links)(l => inFlight.updateAndGet(_ + 1) *> queue.offer(Scrape(l, depth + 1)))
      persist = store.store(Names.toFilename(uri, root), markdown)
      _ <- pushFrontier.zipPar(persist)
    yield ()
