package ma.chinespirit.crawldown

import cats.effect.{Ref, Concurrent}
import cats.effect.std.{Queue, Semaphore, Supervisor, Console}
import cats.{Applicative, ApplicativeError, NonEmptyParallel}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import sttp.model.Uri

final class CatsScraperTF[F[_]: Concurrent: NonEmptyParallel: Console](
    fetch: Fetch[F],
    store: Store[F],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8,
    queueCapacity: Int = 10
):
  def start: F[Unit] =
    for
      queue <- Queue.bounded[F, Scrape | Done | Throwable](queueCapacity)
      semaphore <- Semaphore[F](parallelism.toLong)
      visited <- Ref.of[F, Set[Uri]](Set.empty)
      inFlight <- Ref.of[F, Int](0)
      _ <- queue.offer(Scrape(root, 0))
      _ <- Supervisor[F].use { supervisor =>
        def coordinator: F[Unit] =
          (queue.size, inFlight.get)
            .mapN(_ == 0 && _ == 0)
            .ifM(
              Applicative[F].unit,
              queue.take.flatMap {
                case ex: Throwable => ApplicativeError[F, Throwable].raiseError(ex)
                case Scrape(uri, depth) =>
                  given trace: Trace = Trace(uri)
                  for
                    seen <- visited.get.map(_.contains(uri))
                    _ <-
                      if depth >= maxDepth then Console[F].println(s"$trace: depth limit – skipping $uri")
                      else if seen then Console[F].println(s"$trace: already visited – skipping $uri")
                      else
                        for
                          _ <- visited.update(_ + uri)
                          _ <- inFlight.update(_ + 1)
                          _ <- Console[F].println(s"$trace: CatsScraperTF: crawling $uri")
                          _ <- supervisor.supervise(crawl(uri, depth, queue, semaphore))
                        yield ()
                    _ <- coordinator
                  yield ()
                case Done => inFlight.update(_ - 1) *> coordinator
              }
            )

        coordinator
      }
      _ <- Console[F].println("CatsScraperTF: Finished.")
    yield ()

  private def crawl(
      uri: Uri,
      depth: Int,
      queue: Queue[F, Scrape | Done | Throwable],
      semaphore: Semaphore[F]
  )(using trace: Trace): F[Unit] =
    semaphore.permit
      .use { _ =>
        for
          content <- fetch.fetch(uri)
          (links, markdown) <- ApplicativeError[F, Throwable].fromEither(MdConverter.convertAndExtractLinks(content, uri, selector))
          pushFrontier = links.traverse_(uri => queue.offer(Scrape(uri, depth + 1)))
          persist = store.store(Names.toFilename(uri, root), markdown)
          _ <- (pushFrontier, persist).parTupled
        yield ()
      }
      .handleErrorWith { case ex => queue.offer(ex) }
      .guarantee(queue.offer(Done))
      .onCancel(Console[F].println(s"$trace: CatsScraperTF: cancelled $uri"))
