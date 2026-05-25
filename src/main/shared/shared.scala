package ma.chinespirit.crawldown

import cats.effect.IO
import cats.effect.Sync
import cats.syntax.all.*
import kyo.{Async as KyoAsync, Sync as KyoSync, Result as _, *}
import sttp.client4.quick.*
import sttp.model.Uri
import scala.util.Try
import zio.Task as ZTask
import zio.ZIO
import turbolift.!!
import turbolift.effects.IO as TIO
import steps.result.*

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking

// given Conversion[Throwable, Throwable] = t => t

extension [E, A](either: Either[E, A])
  def toResult: Result[A, E] =
    either match
      case Left(err)    => Result.Err(err)
      case Right(value) => Result.Ok(value)

type ResultEx[+A] = Result[A, Throwable]

type EitherErrorOr[+A] = Either[Throwable, A]

type Kyo[+A] = A < (KyoAsync & Abort[Throwable])

type TTask[+A] = A !! TIO

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

  def turbolift: Fetch[TTask] = new Fetch[TTask]:
    def fetch(uri: Uri): String !! TIO =
      TIO.blocking(basicRequest.get(uri).send()).flatMap { response =>
        response.body match
          case Left(error) =>
            TIO.raise(Exception(s"Failed to fetch $uri: $error"))
          case Right(body) => !!.pure(body)
      }

  def either: Fetch[EitherErrorOr] = new Fetch[EitherErrorOr]:
    def fetch(uri: Uri): EitherErrorOr[String] =
      val response =
        try basicRequest.get(uri).send().body
        catch case ex => Left(ex.getMessage())

      response match
        case Left(error) =>
          Left(Exception(s"Failed to fetch $uri: $error"))
        case Right(body) => Right(body)

  def result: Fetch[ResultEx] = new Fetch[ResultEx]:
    def fetch(uri: Uri): ResultEx[String] =
      val response =
        try basicRequest.get(uri).send().body
        catch case ex => Left(ex)

      response match
        case Left(error) => Result.Err(Exception(s"Failed to fetch $uri: $error"))
        case Right(body) => Result.Ok(body)

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

  def turbolift(dir: os.Path): Store[TTask] =
    os.makeDir.all(dir)

    new Store[TTask]:
      def store(key: String, value: String): Unit !! TIO =
        TIO.blocking(os.write(dir / key, value))

  def either(dir: os.Path): Store[EitherErrorOr] =
    os.makeDir.all(dir)

    new Store[EitherErrorOr]:
      def store(key: String, value: String): EitherErrorOr[Unit] =
        Try(os.write(dir / key, value)).toEither

  def result(dir: os.Path): Store[ResultEx] =
    os.makeDir.all(dir)

    new Store[ResultEx]:
      def store(key: String, value: String): ResultEx[Unit] =
        Try(os.write(dir / key, value)).toEither.toResult

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
