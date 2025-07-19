package ma.chinespirit.crawldown

import cats.effect.IO
import cats.effect.Resource
import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import kyo.<
import kyo.Clock as KyoClock
import kyo.Scope as KyoScope
import kyo.Sync
import sttp.model.Uri
import zio.Clock
import zio.Scope
import zio.Task as ZTask
import zio.ZIO

import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

case class Trace(id: UUID, uri: Option[Uri])
object Trace:
  def apply(uri: Option[Uri]): Trace = Trace(UUID.randomUUID(), uri)

type Id[A] = A

case class SpanData(
    id: UUID,
    parentId: Option[UUID],
    name: String,
    uri: Option[String],
    startTime: Instant,
    endTime: Instant
)

object SpanData:
  given JsonValueCodec[Uri] = new JsonValueCodec[Uri]:
    def decodeValue(in: JsonReader, default: Uri): Uri =
      Uri.parse(in.readString(null)).getOrElse(default)

    def encodeValue(x: Uri, out: JsonWriter): Unit = out.writeVal(x.toString)
    def nullValue: Uri = null
  given JsonValueCodec[SpanData] = JsonCodecMaker.make
  given JsonValueCodec[Seq[SpanData]] = JsonCodecMaker.make

trait Tracing[F[_]]:
  def span[A](name: String)(body: Trace ?=> F[A])(using parent: Trace): F[A]
  def root[A](name: String)(body: Trace ?=> F[A]): F[A]

object Tracing:

  class FutureTracing(spans: ConcurrentLinkedQueue[SpanData])(using ExecutionContext) extends Tracing[Future]:
    def span[A](n: String)(body: Trace ?=> Future[A])(using p: Trace): Future[A] =
      val childTrace = Trace(p.uri)
      val start = Instant.now()
      body(using childTrace).andThen { case _ =>
        spans.add(SpanData(childTrace.id, Some(p.id), n, p.uri.map(_.toString), start, Instant.now()))
      }
    def root[A](n: String)(body: Trace ?=> Future[A]): Future[A] =
      val rootTrace = Trace(None)
      val start = Instant.now()
      body(using rootTrace).andThen { case _ =>
        spans.add(SpanData(rootTrace.id, None, n, None, start, Instant.now()))
      }

  class CatsTracing(spans: ConcurrentLinkedQueue[SpanData]) extends Tracing[IO]:
    private def now: IO[Instant] = IO.realTime.map(d => Instant.ofEpochMilli(d.toMillis))
    def span[A](n: String)(body: Trace ?=> IO[A])(using p: Trace): IO[A] =
      val childTrace = Trace(p.uri)
      for
        start <- now
        res <- body(using childTrace).guarantee(
          now.map(end => spans.add(SpanData(childTrace.id, Some(p.id), n, p.uri.map(_.toString), start, end)))
        )
      yield res
    def root[A](n: String)(body: Trace ?=> IO[A]): IO[A] =
      val rootTrace = Trace(None)
      for
        start <- now
        res <- body(using rootTrace).guarantee(
          now.map(end => spans.add(SpanData(rootTrace.id, None, n, None, start, end)))
        )
      yield res

  class ZioTracing(spans: ConcurrentLinkedQueue[SpanData]) extends Tracing[ZTask]:
    def span[A](n: String)(body: Trace ?=> ZTask[A])(using p: Trace): ZTask[A] =
      val childTrace = Trace(p.uri)
      for
        start <- Clock.instant
        res <- body(using childTrace).ensuring(
          Clock.instant.map(end => spans.add(SpanData(childTrace.id, Some(p.id), n, p.uri.map(_.toString), start, end)))
        )
      yield res
    def root[A](n: String)(body: Trace ?=> ZTask[A]): ZTask[A] =
      val rootTrace = Trace(None)
      for
        start <- Clock.instant
        res <- body(using rootTrace).ensuring(
          Clock.instant.map(end => spans.add(SpanData(rootTrace.id, None, n, None, start, end)))
        )
      yield res

  class KyoTracing(spans: ConcurrentLinkedQueue[SpanData]) extends Tracing[Kyo]:
    def span[A](n: String)(body: Trace ?=> Kyo[A])(using p: Trace): Kyo[A] =
      val childTrace = Trace(p.uri)
      KyoScope.run {
        for
          start <- KyoClock.now
          _ <- KyoScope.ensure(KyoClock.now.map { end =>
            spans.add(SpanData(childTrace.id, Some(p.id), n, p.uri.map(_.toString), start.toJava, end.toJava))
          })
          res <- body(using childTrace)
        yield res
      }
    def root[A](n: String)(body: Trace ?=> Kyo[A]): Kyo[A] =
      val rootTrace = Trace(None)
      KyoScope.run {
        for
          start <- KyoClock.now
          _ <- KyoScope.ensure(
            KyoClock.now.map { end =>
              spans.add(SpanData(rootTrace.id, None, n, None, start.toJava, end.toJava))
            }
          )
          res <- body(using rootTrace)
        yield res
      }

  class SyncTracing(spans: ConcurrentLinkedQueue[SpanData]) extends Tracing[Id]:
    def span[A](n: String)(body: Trace ?=> Id[A])(using p: Trace): Id[A] =
      val childTrace = Trace(p.uri)
      val start = Instant.now()
      try body(using childTrace)
      finally spans.add(SpanData(childTrace.id, Some(p.id), n, p.uri.map(_.toString), start, Instant.now()))
    def root[A](n: String)(body: Trace ?=> Id[A]): Id[A] =
      val rootTrace = Trace(None)
      val start = Instant.now()
      try body(using rootTrace)
      finally spans.add(SpanData(rootTrace.id, None, n, None, start, Instant.now()))

  private def writeSpans(name: String, spans: ConcurrentLinkedQueue[SpanData]): Unit =
    os.makeDir.all(os.pwd / "traces")
    val file = os.pwd / "traces" / s"$name-traces.json"
    os.write(file, writeToString(spans.asScala.toSeq.reverse))

  def future(name: String)(using ExecutionContext): (Tracing[Future], () => Unit) =
    val spans = ConcurrentLinkedQueue[SpanData]()
    (FutureTracing(spans), () => writeSpans(name, spans))

  def cats(name: String): Resource[IO, Tracing[IO]] =
    val spans = ConcurrentLinkedQueue[SpanData]()
    Resource.make(IO.pure(CatsTracing(spans)))(_ => IO.blocking(writeSpans(name, spans)))

  def zio(name: String): ZIO[Scope, Nothing, Tracing[ZTask]] =
    val spans = ConcurrentLinkedQueue[SpanData]()
    ZIO.acquireRelease(ZIO.succeed(ZioTracing(spans)))(_ => ZIO.attemptBlocking(writeSpans(name, spans)).orDie)

  def kyo(name: String): Tracing[Kyo] < (KyoScope & Sync) =
    val spans = ConcurrentLinkedQueue[SpanData]()
    KyoScope.ensure(Sync.defer(writeSpans(name, spans))).andThen(KyoTracing(spans))

  def sync(name: String): (Tracing[Id], () => Unit) =
    val spans = ConcurrentLinkedQueue[SpanData]()
    (SyncTracing(spans), () => writeSpans(name, spans))
