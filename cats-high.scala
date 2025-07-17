package ma.chinespirit.crawldown

import cats.effect.{IO, Ref}
import cats.effect.std.Queue
import cats.syntax.all.*
import sttp.model.Uri

final class CatsScraperHighLevel(
    fetch: Fetch[IO],
    store: Store[IO],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
):

  def start: IO[Unit] =
    for
      queue <- Queue.unbounded[IO, Scrape | Done]
      visited <- Ref.of[IO, Set[Uri]](Set.empty)
      inFlight <- Ref.of[IO, Int](0)
      _ <- inFlight.update(_ + 1)
      _ <- queue.offer(Scrape(root, 0))
      _ <- IO.parSequenceN_(parallelism)(Vector.fill(parallelism)(worker(queue, visited, inFlight)))
    yield ()

  private def worker(queue: Queue[IO, Scrape | Done], visited: Ref[IO, Set[Uri]], inFlight: Ref[IO, Int]): IO[Unit] =
    queue.take.flatMap {
      case Done => IO.unit
      case Scrape(uri, depth) =>
        given trace: Trace = Trace(uri)
        val handleUri =
          if depth >= maxDepth then IO.unit
          else
            visited.getAndUpdate(_ + uri).flatMap { visitedSet =>
              if visitedSet.contains(uri) then IO.unit
              else crawl(uri, depth, queue, inFlight)
            }

        handleUri *> inFlight.updateAndGet(_ - 1).flatMap { currentInFlight =>
          if currentInFlight > 0 then worker(queue, visited, inFlight)
          else Vector.fill(parallelism)(Done).map(queue.offer).sequence.void
        }
    }

  private def crawl(uri: Uri, depth: Int, queue: Queue[IO, Scrape | Done], inFlight: Ref[IO, Int])(using
      trace: Trace
  ): IO[Unit] =
    for
      content <- fetch.fetch(uri)
      (links, markdown) <- IO.fromEither(MdConverter.convertAndExtractLinks(content, uri, selector))
      pushFrontier = links.traverse_(uri => inFlight.updateAndGet(_ + 1) *> queue.offer(Scrape(uri, depth + 1)))
      persist = store.store(Names.toFilename(uri, root), markdown)
      _ <- (pushFrontier, persist).parTupled
    yield ()
