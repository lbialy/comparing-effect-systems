package ma.chinespirit.crawldown

import cats.effect.{Ref, Concurrent}
import cats.effect.std.Queue
import cats.*
import cats.syntax.all.*
import sttp.model.Uri

final class CatsScraperHighLevelTF[F[_]: Concurrent: NonEmptyParallel](
    fetch: Fetch[F],
    store: Store[F],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
):
  def start: F[Unit] =
    for
      queue <- Queue.unbounded[F, Scrape | Done]
      visited <- Ref.of[F, Set[Uri]](Set.empty)
      inFlight <- Ref.of[F, Int](0)
      _ <- inFlight.update(_ + 1)
      _ <- queue.offer(Scrape(root, 0))
      _ <- Concurrent[F].parSequenceN_(parallelism)(Vector.fill(parallelism)(worker(queue, visited, inFlight)))
    yield ()

  private def worker(queue: Queue[F, Scrape | Done], visited: Ref[F, Set[Uri]], inFlight: Ref[F, Int]): F[Unit] =
    queue.take.flatMap {
      case Done => Applicative[F].unit
      case Scrape(uri, depth) =>
        val handleUri =
          if depth >= maxDepth then Applicative[F].unit
          else
            visited.getAndUpdate(_ + uri).flatMap { visitedSet =>
              if visitedSet.contains(uri) then Applicative[F].unit
              else crawl(uri, depth, queue, inFlight)
            }

        handleUri *> inFlight.updateAndGet(_ - 1).flatMap { currentInFlight =>
          if currentInFlight > 0 then worker(queue, visited, inFlight)
          else Vector.fill(parallelism)(Done).map(queue.offer).sequence.void
        }
    }

  private def crawl(uri: Uri, depth: Int, queue: Queue[F, Scrape | Done], inFlight: Ref[F, Int]): F[Unit] =
    for
      content <- fetch.fetch(uri)
      (links, markdown) <- MonadError[F, Throwable].fromEither(MdConverter.convertAndExtractLinks(content, uri, selector))
      pushFrontier = links.traverse_(uri => inFlight.updateAndGet(_ + 1) *> queue.offer(Scrape(uri, depth + 1)))
      persist = store.store(Names.toFilename(uri, root), markdown)
      _ <- (pushFrontier, persist).parTupled
    yield ()
