package ma.chinespirit.crawldown

import cats.effect.{Sync, Ref, Concurrent}
import cats.effect.kernel.GenSpawn
import cats.effect.std.{Console, Queue}
import cats.*
import cats.syntax.all.*
import sttp.model.Uri

final class CatsScraperHighLevelTF[F[_]: Concurrent: NonEmptyParallel: Console](
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
      _ <- Console[F].println("CatsScraperHighLevelTF: Finished.")
    yield ()

  private def worker(queue: Queue[F, Scrape | Done], visited: Ref[F, Set[Uri]], inFlight: Ref[F, Int]): F[Unit] =
    queue.take.flatMap {
      case Done => Console[F].println("CatsScraperHighLevelTF: Received poison pill.") *> Monad[F].unit
      case Scrape(uri, depth) =>
        given trace: Trace = Trace(uri)
        val handleUri =
          if depth >= maxDepth then Console[F].println(s"$trace: CatsScraperHighLevelTF: depth limit – skipping $uri")
          else
            visited.getAndUpdate(_ + uri).flatMap { visitedSet =>
              if visitedSet.contains(uri) then Console[F].println(s"$trace: CatsScraperHighLevelTF: already visited – skipping $uri")
              else crawl(uri, depth, queue, inFlight)
            }

        handleUri *> inFlight.updateAndGet(_ - 1).flatMap { currentInFlight =>
          if currentInFlight > 0 then worker(queue, visited, inFlight)
          else Vector.fill(parallelism)(Done).map(queue.offer).sequence.void
        }
    }

  private def crawl(uri: Uri, depth: Int, queue: Queue[F, Scrape | Done], inFlight: Ref[F, Int])(using
      trace: Trace
  ): F[Unit] =
    for
      _ <- Console[F].println(s"$trace: CatsScraperHighLevelTF: crawling $uri")
      content <- fetch.fetch(uri)
      (links, markdown) <- MonadError[F, Throwable].fromEither(MdConverter.convertAndExtractLinks(content, uri, selector))
      pushFrontier = links.traverse_(uri => inFlight.updateAndGet(_ + 1) *> queue.offer(Scrape(uri, depth + 1)))
      persist = store.store(Names.toFilename(uri, root), markdown)
      _ <- (pushFrontier, persist).parTupled
    yield ()
