package ma.chinespirit.crawldown

import cats.effect.IO
import cats.effect.Sync
import cats.syntax.all.*
import kyo.{Async as KyoAsync, Sync as KyoSync, *}
import sttp.client4.quick.*
import sttp.model.Uri
import zio.Task as ZTask
import zio.ZIO

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking

type Result[+A] = Either[Throwable, A]

type Kyo[+A] = A < (KyoAsync & KyoSync & Abort[Throwable])

case class Scrape(uri: Uri, depth: Int)
sealed trait Done
case object Done extends Done

trait Fetch[F[_]]:
  def fetch(uri: Uri): F[String]

object Fetch:
  def future(using ExecutionContext): Fetch[Future] =
    new Fetch[Future]:
      def fetch(uri: Uri): Future[String] =
        Future:
          blocking:
            val response = basicRequest.get(uri).send()
            response.body match
              case Left(error) =>
                throw Exception(s"Failed to fetch $uri: $error")
              case Right(body) => body

  def cats: Fetch[IO] = new Fetch[IO]:
    def fetch(uri: Uri): IO[String] =
      IO.blocking(basicRequest.get(uri).send()).flatMap { response =>
        response.body match
          case Left(error) =>
            IO.raiseError(Exception(s"Failed to fetch $uri: $error"))
          case Right(body) => IO.pure(body)
      }

  def catsTF[F[_]: Sync]: Fetch[F] = new Fetch[F]:
    def fetch(uri: Uri): F[String] =
      Sync[F].blocking(basicRequest.get(uri).send()).flatMap { response =>
        response.body match
          case Left(error) =>
            Sync[F].raiseError(Exception(s"Failed to fetch $uri: $error"))
          case Right(body) => Sync[F].pure(body)
      }

  def zio: Fetch[ZTask] = new Fetch[ZTask]:
    def fetch(uri: Uri): ZTask[String] =
      ZIO.attemptBlocking(basicRequest.get(uri).send()).flatMap { response =>
        response.body match
          case Left(error) =>
            ZIO.fail(Exception(s"Failed to fetch $uri: $error"))
          case Right(body) => ZIO.succeed(body)
      }

  def kyo: Fetch[Kyo] = new Fetch[Kyo]:
    def fetch(uri: Uri): Kyo[String] =
      KyoSync.defer(basicRequest.get(uri).send()).flatMap { response =>
        response.body match
          case Left(error) =>
            Abort.fail(Exception(s"Failed to fetch $uri: $error"))
          case Right(body) => KyoSync.defer(body)
      }

  def sync: Fetch[Result] = new Fetch[Result]:
    def fetch(uri: Uri): Result[String] =
      val response = basicRequest.get(uri).send()
      response.body match
        case Left(error) =>
          Left(Exception(s"Failed to fetch $uri: $error"))
        case Right(body) => Right(body)

trait Store[F[_]]:
  def store(key: String, value: String): F[Unit]

object Store:
  def future(dir: os.Path)(using ExecutionContext): Store[Future] =
    os.makeDir.all(dir)

    new Store[Future]:
      def store(key: String, value: String): Future[Unit] =
        Future:
          blocking:
            os.write(dir / key, value)

  def cats(dir: os.Path): Store[IO] =
    os.makeDir.all(dir)

    new Store[IO]:
      def store(key: String, value: String): IO[Unit] =
        IO.blocking(os.write(dir / key, value))

  def catsTF[F[_]: Sync](dir: os.Path): Store[F] =
    os.makeDir.all(dir)

    new Store[F]:
      def store(key: String, value: String): F[Unit] =
        Sync[F].blocking(os.write(dir / key, value))

  def zio(dir: os.Path): Store[ZTask] =
    os.makeDir.all(dir)

    new Store[ZTask]:
      def store(key: String, value: String): ZTask[Unit] =
        ZIO.attemptBlocking(os.write(dir / key, value))

  def kyo(dir: os.Path): Store[Kyo] =
    os.makeDir.all(dir)

    new Store[Kyo]:
      def store(key: String, value: String): Kyo[Unit] =
        KyoSync.defer(Abort.catching(os.write(dir / key, value)))

  def sync(dir: os.Path): Store[Result] =
    os.makeDir.all(dir)

    new Store[Result]:
      def store(key: String, value: String): Result[Unit] =
        os.write(dir / key, value)
        Right(())

object Names:
  def toDirname(uri: Uri): String =
    val host = uri.host
      .getOrElse {
        throw RuntimeException(s"No host found in URI: $uri")
      }
    val path = uri.path.mkString("_")

    s"${host}_$path"
      .replaceAll("[^\\.a-zA-Z0-9_-]", "")
      .stripPrefix("_")
      .stripSuffix("_")

  def toFilename(uri: Uri, baseUri: Uri): String =
    val remainingPath = uri.path.zipAll(baseUri.path, "", "").dropWhile((a, b) => a == b).map(_._1)

    val path = remainingPath
      .mkString("_")
      .replaceAll("/", "_")
      .replaceAll("[^\\.a-zA-Z0-9_-]", "")
      .stripPrefix("_")
      .stripSuffix("_")

    if path.isBlank then "index.md" else s"$path.md"
