package ma.chinespirit.crawldown

import sttp.model.Uri
import java.util.UUID
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import scala.concurrent.{ExecutionContext, Future}
import cats.effect.{IO, Resource, Temporal}
import cats.syntax.all.*
import zio.{ZIO, ZIOAppArgs, Scope, Clock, Task => ZTask}
import kyo.{IO => KyoIO, Async => KyoAsync, Fiber => KyoFiber, Result => KyoResult, Clock => KyoClock}
import gears.async.{Async, AsyncSupport, Future as GearsFuture, Listener}
import ox.supervised

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
  private def newTrace(uri: Option[Uri]): Trace = Trace(uri)

  class FutureTracing(name: String, spans: ConcurrentLinkedQueue[SpanData])(using ExecutionContext) extends Tracing[Future]:
    def span[A](n: String)(body: Trace ?=> Future[A])(using p: Trace): Future[A] =
      val childTrace = newTrace(p.uri)
      val start = Instant.now()
      body(using childTrace).andThen { case _ =>
        spans.add(SpanData(childTrace.id, Some(p.id), n, p.uri.map(_.toString), start, Instant.now()))
      }
    def root[A](n: String)(body: Trace ?=> Future[A]): Future[A] =
      val rootTrace = newTrace(None)
      val start = Instant.now()
      body(using rootTrace).andThen { case _ =>
        spans.add(SpanData(rootTrace.id, None, n, None, start, Instant.now()))
      }

  class CatsTracing(name: String, spans: ConcurrentLinkedQueue[SpanData]) extends Tracing[IO]:
    private def now: IO[Instant] = IO.realTime.map(d => Instant.ofEpochMilli(d.toMillis))
    def span[A](n: String)(body: Trace ?=> IO[A])(using p: Trace): IO[A] =
      val childTrace = newTrace(p.uri)
      for
        start <- now
        res <- body(using childTrace).guarantee(
          now.map(end => spans.add(SpanData(childTrace.id, Some(p.id), n, p.uri.map(_.toString), start, end)))
        )
      yield res
    def root[A](n: String)(body: Trace ?=> IO[A]): IO[A] =
      val rootTrace = newTrace(None)
      for
        start <- now
        res <- body(using rootTrace).guarantee(
          now.map(end => spans.add(SpanData(rootTrace.id, None, n, None, start, end)))
        )
      yield res

  class ZioTracing(name: String, spans: ConcurrentLinkedQueue[SpanData]) extends Tracing[ZTask]:
    def span[A](n: String)(body: Trace ?=> ZTask[A])(using p: Trace): ZTask[A] =
      val childTrace = newTrace(p.uri)
      for
        start <- Clock.instant
        res <- body(using childTrace).ensuring(
          Clock.instant.map(end => spans.add(SpanData(childTrace.id, Some(p.id), n, p.uri.map(_.toString), start, end)))
        )
      yield res
    def root[A](n: String)(body: Trace ?=> ZTask[A]): ZTask[A] =
      val rootTrace = newTrace(None)
      for
        start <- Clock.instant
        res <- body(using rootTrace).ensuring(
          Clock.instant.map(end => spans.add(SpanData(rootTrace.id, None, n, None, start, end)))
        )
      yield res

  class KyoTracing(name: String, spans: ConcurrentLinkedQueue[SpanData]) extends Tracing[Kyo]:
    def span[A](n: String)(body: Trace ?=> Kyo[A])(using p: Trace): Kyo[A] =
      val childTrace = newTrace(p.uri)
      for
        start <- KyoClock.now
        res <- KyoIO.ensure(
          KyoClock.now.map(end => spans.add(SpanData(childTrace.id, Some(p.id), n, p.uri.map(_.toString), start.toJava, end.toJava)))
        )(
          body(using childTrace)
        )
      yield res
    def root[A](n: String)(body: Trace ?=> Kyo[A]): Kyo[A] =
      val rootTrace = newTrace(None)
      for
        start <- KyoClock.now
        res <- KyoIO.ensure(
          KyoClock.now.map(end => spans.add(SpanData(rootTrace.id, None, n, None, start.toJava, end.toJava)))
        )(
          body(using rootTrace)
        )
      yield res

  class OxTracing(name: String, spans: ConcurrentLinkedQueue[SpanData]) extends Tracing[Result]:
    def span[A](n: String)(body: Trace ?=> Result[A])(using p: Trace): Result[A] =
      val childTrace = newTrace(p.uri)
      val start = Instant.now()
      val res = body(using childTrace)
      spans.add(SpanData(childTrace.id, Some(p.id), n, p.uri.map(_.toString), start, Instant.now()))
      res
    def root[A](n: String)(body: Trace ?=> Result[A]): Result[A] =
      val rootTrace = newTrace(None)
      val start = Instant.now()
      val res = body(using rootTrace)
      spans.add(SpanData(rootTrace.id, None, n, None, start, Instant.now()))
      res

  class GearsTracing(name: String, spans: ConcurrentLinkedQueue[SpanData])(using Async) extends Tracing[GearsFuture]:
    def span[A](n: String)(body: Trace ?=> GearsFuture[A])(using p: Trace): GearsFuture[A] =
      val childTrace = newTrace(p.uri)
      val start = Instant.now()
      val fut = body(using childTrace)
      fut.onComplete(
        Listener((_, _) => spans.add(SpanData(childTrace.id, Some(p.id), n, p.uri.map(_.toString), start, Instant.now())))
      )
      fut
    def root[A](n: String)(body: Trace ?=> GearsFuture[A]): GearsFuture[A] =
      val rootTrace = newTrace(None)
      val start = Instant.now()
      val fut = body(using rootTrace)
      fut.onComplete(Listener((_, _) => spans.add(SpanData(rootTrace.id, None, n, None, start, Instant.now()))))
      fut

  private def writeSpans(name: String, spans: ConcurrentLinkedQueue[SpanData]): Unit =
    val file = os.pwd / s"$name-traces.json"
    os.write(file, writeToString(spans.asScala.toSeq))

  def future(name: String)(using ExecutionContext): (Tracing[Future], () => Unit) =
    val spans = ConcurrentLinkedQueue[SpanData]()
    (FutureTracing(name, spans), () => writeSpans(name, spans))

  def cats(name: String): Resource[IO, Tracing[IO]] =
    val spans = ConcurrentLinkedQueue[SpanData]()
    Resource.make(IO.pure(CatsTracing(name, spans)))(_ => IO.blocking(writeSpans(name, spans)))

  def zio(name: String): ZIO[Scope, Nothing, Tracing[ZTask]] =
    val spans = ConcurrentLinkedQueue[SpanData]()
    ZIO.acquireRelease(ZIO.succeed(ZioTracing(name, spans)))(_ => ZIO.attemptBlocking(writeSpans(name, spans)).orDie)

  def kyo(name: String): Kyo[Tracing[Kyo]] =
    val spans = ConcurrentLinkedQueue[SpanData]()
    KyoIO.ensure(KyoIO(writeSpans(name, spans)))(KyoTracing(name, spans))

  def ox(name: String): (Tracing[Result], () => Unit) =
    val spans = ConcurrentLinkedQueue[SpanData]()
    (OxTracing(name, spans), () => writeSpans(name, spans))

  def gears(name: String)(using Async): (Tracing[GearsFuture], () => Unit) =
    val spans = ConcurrentLinkedQueue[SpanData]()
    (GearsTracing(name, spans), () => writeSpans(name, spans))
