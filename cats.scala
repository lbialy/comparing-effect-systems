package ma.chinespirit.crawldown

import cats.effect.{IO, Ref}
import cats.effect.std.{Queue, Semaphore, Supervisor}
import cats.syntax.all.*
import sttp.model.Uri

final class CatsScraper(
    fetch: Fetch[IO],
    store: Store[IO],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8,
    queueCapacity: Int = 10
):

  def start: IO[Unit] =
    for
      queue <- Queue.bounded[IO, Scrape | Done | Throwable](queueCapacity)
      semaphore <- Semaphore[IO](parallelism.toLong)
      visited <- Ref.of[IO, Set[Uri]](Set.empty)
      inFlight <- Ref.of[IO, Int](0)
      _ <- queue.offer(Scrape(root, 0))
      _ <- Supervisor[IO].use { supervisor =>
        def coordinator: IO[Unit] =
          (queue.size, inFlight.get)
            .mapN(_ == 0 && _ == 0)
            .ifM(
              IO.unit,
              queue.take.flatMap {
                case ex: Throwable => IO.raiseError(ex)
                case Scrape(uri, depth) =>
                  for
                    seen <- visited.get.map(_.contains(uri))
                    _ <-
                      if depth >= maxDepth then IO.unit
                      else if seen then IO.unit
                      else
                        for
                          _ <- visited.update(_ + uri)
                          _ <- inFlight.update(_ + 1)
                          _ <- supervisor.supervise(crawl(uri, depth, queue, semaphore))
                        yield ()
                    _ <- coordinator
                  yield ()
                case Done => inFlight.update(_ - 1) *> coordinator
              }
            )

        coordinator
      }
    yield ()

  private def crawl(uri: Uri, depth: Int, queue: Queue[IO, Scrape | Done | Throwable], semaphore: Semaphore[IO]): IO[Unit] =
    semaphore.permit
      .use { _ =>
        for
          content <- fetch.fetch(uri)
          (links, markdown) <- IO.fromEither(MdConverter.convertAndExtractLinks(content, uri, selector))
          pushFrontier = links.traverse_(uri => queue.offer(Scrape(uri, depth + 1)))
          persist = store.store(Names.toFilename(uri, root), markdown)
          _ <- (pushFrontier, persist).parTupled
        yield ()
      }
      .handleErrorWith { case ex => queue.offer(ex) }
      .guarantee(queue.offer(Done))
