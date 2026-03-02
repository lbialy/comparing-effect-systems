package ma.chinespirit.crawldown

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.plokhotnyuk.jsoniter_scala.core.*
import munit.FunSuite
import zio.Runtime
import zio.Unsafe
import zio.ZIO

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.*

class TracingTests extends FunSuite {
  given ExecutionContext = ExecutionContext.global

  override def beforeAll(): Unit =
    os.remove.all(os.pwd / "traces")

  def readSpans(name: String): Seq[SpanData] =
    val file = os.pwd / "traces" / s"$name-traces.json"
    if os.exists(file) then
      val json = os.read(file)
      readFromString[Seq[SpanData]](json)
    else Seq.empty

  def verifySpans(spans: Seq[SpanData], rootName: String, children: Map[String, Seq[String]]): Unit =
    val rootSpans = spans.filter(_.parentId.isEmpty)
    assertEquals(rootSpans.size, 1, "There should be exactly one root span")
    val root = rootSpans.head
    assertEquals(root.name, rootName, "Root span name is incorrect")

    def checkChildren(parentId: java.util.UUID, expectedChildren: Seq[String]): Unit =
      val childSpans = spans.filter(_.parentId.contains(parentId))
      assertEquals(childSpans.map(_.name).sorted, expectedChildren.sorted, s"Children of span $parentId are incorrect")
      childSpans.foreach { child =>
        children.get(child.name).foreach { grandChildren =>
          checkChildren(child.id, grandChildren)
        }
      }

    children.get(rootName).foreach(directChildren => checkChildren(root.id, directChildren))

  // Future Tests
  test("Future: single root span") {
    val (tracing, close) = Tracing.future("future-single")
    val result = tracing.root("root") {
      Future.successful("done")
    }
    result.map { _ =>
      close()
      val spans = readSpans("future-single")
      verifySpans(spans, "root", Map.empty)
    }
  }

  test("Future: nested spans") {
    val (tracing, close) = Tracing.future("future-nested")
    tracing
      .root("root") {
        tracing
          .span("child1") {
            Future.successful("child1 done")
          }
          .flatMap { _ =>
            tracing.span("child2") {
              Future.successful("child2 done")
            }
          }
      }
      .map { _ =>
        close()
        val spans = readSpans("future-nested")
        verifySpans(spans, "root", Map("root" -> Seq("child1", "child2")))
      }
  }

  // Cats Effect Tests
  test("Cats: single root span") {
    Tracing
      .cats("cats-single")
      .use { tracing =>
        tracing.root("root") {
          IO.pure("done")
        }
      }
      .unsafeRunSync()
    val spans = readSpans("cats-single")
    verifySpans(spans, "root", Map.empty)
  }

  test("Cats: nested spans") {
    Tracing
      .cats("cats-nested")
      .use { tracing =>
        tracing.root("root") {
          tracing.span("child1") {
            IO.unit
          } >>
            tracing.span("child2") {
              tracing.span("grandchild1") {
                IO.unit
              }
            }
        }
      }
      .unsafeRunSync()
    val spans = readSpans("cats-nested")
    verifySpans(spans, "root", Map("root" -> Seq("child1", "child2"), "child2" -> Seq("grandchild1")))
  }

  // ZIO Tests
  test("ZIO: single root span") {
    Unsafe.unsafely {
      Runtime.default.unsafe.run(
        ZIO.scoped {
          Tracing.zio("zio-single").flatMap { tracing =>
            tracing.root("root") {
              ZIO.succeed("done")
            }
          }
        }
      )
    }
    val spans = readSpans("zio-single")
    verifySpans(spans, "root", Map.empty)
  }

  test("ZIO: nested spans") {
    Unsafe.unsafely {
      Runtime.default.unsafe.run(
        ZIO.scoped {
          Tracing.zio("zio-nested").flatMap { tracing =>
            tracing.root("root") {
              tracing.span("child1") {
                ZIO.succeed("done")
              } *>
                tracing.span("child2") {
                  tracing.span("grandchild1") {
                    ZIO.succeed("done")
                  }
                }
            }
          }
        }
      )
    }
    val spans = readSpans("zio-nested")
    verifySpans(spans, "root", Map("root" -> Seq("child1", "child2"), "child2" -> Seq("grandchild1")))
  }

  // Kyo Tests
  test("Kyo: single root span") {
    import kyo.AllowUnsafe.embrace.danger
    import kyo.*
    KyoApp.Unsafe.runAndBlock(kyo.Duration.Infinity) {
      Tracing.kyo("kyo-single").map { tracing =>
        tracing.root("root") {
          Sync.defer("done")
        }
      }
    }
    val spans = readSpans("kyo-single")
    verifySpans(spans, "root", Map.empty)
  }

  test("Kyo: nested spans") {
    import kyo.AllowUnsafe.embrace.danger
    import kyo.*
    KyoApp.Unsafe.runAndBlock(kyo.Duration.Infinity) {
      Tracing.kyo("kyo-nested").map { tracing =>
        tracing.root("root") {
          for {
            _ <- tracing.span("child1") { Kyo.unit }
            _ <- tracing.span("child2") {
              tracing.span("grandchild1") { Kyo.unit }
            }
          } yield ()
        }
      }
    }
    val spans = readSpans("kyo-nested")
    verifySpans(spans, "root", Map("root" -> Seq("child1", "child2"), "child2" -> Seq("grandchild1")))
  }

  // Sync Tests
  test("Sync: single root span") {
    val (tracing, close) = Tracing.sync("ox-single")
    tracing.root("root") {
      "done"
    }
    close()
    val spans = readSpans("ox-single")
    verifySpans(spans, "root", Map.empty)
  }

  test("Sync: nested spans") {
    val (tracing, close) = Tracing.sync("ox-nested")
    tracing.root("root") {
      tracing.span("child1") { "done child1" }
      tracing.span("child2") {
        tracing.span("grandchild1") { "done grandchild1" }
      }
    }
    close()
    val spans = readSpans("ox-nested")
    verifySpans(spans, "root", Map("root" -> Seq("child1", "child2"), "child2" -> Seq("grandchild1")))
  }

  // Concurrency and Error Tests
  test("Future: concurrent spans") {
    val (tracing, close) = Tracing.future("future-concurrent")
    tracing
      .root("root") {
        val f1 = tracing.span("fiber1") { Future { Thread.sleep(10); "f1" } }
        val f2 = tracing.span("fiber2") { Future { Thread.sleep(20); "f2" } }
        Future.sequence(Seq(f1, f2))
      }
      .map { _ =>
        close()
        val spans = readSpans("future-concurrent")
        verifySpans(spans, "root", Map("root" -> Seq("fiber1", "fiber2")))
      }
  }

  test("Cats: concurrent spans") {
    Tracing
      .cats("cats-concurrent")
      .use { tracing =>
        tracing.root("root") {
          val f1 = tracing.span("fiber1") { IO.sleep(10.millis) }
          val f2 = tracing.span("fiber2") { IO.sleep(20.millis) }
          (f1, f2).parMapN((_, _) => ())
        }
      }
      .unsafeRunSync()
    val spans = readSpans("cats-concurrent")
    verifySpans(spans, "root", Map("root" -> Seq("fiber1", "fiber2")))
  }

  test("ZIO: concurrent spans") {
    Unsafe.unsafely {
      Runtime.default.unsafe.run(
        ZIO.scoped {
          Tracing.zio("zio-concurrent").flatMap { tracing =>
            tracing.root("root") {
              val f1 = tracing.span("fiber1") { ZIO.sleep(zio.Duration.fromMillis(10)) }
              val f2 = tracing.span("fiber2") { ZIO.sleep(zio.Duration.fromMillis(20)) }
              ZIO.collectAllPar(Seq(f1, f2))
            }
          }
        }
      )
    }
    val spans = readSpans("zio-concurrent")
    verifySpans(spans, "root", Map("root" -> Seq("fiber1", "fiber2")))
  }

  test("Future: no span on failure") {
    val (tracing, close) = Tracing.future("future-failure")
    val result = tracing.root("root") {
      tracing.span("fail") {
        Future.failed(new RuntimeException("error"))
      }
    }
    result.recover { case _ =>
      close()
      val spans = readSpans("future-failure")
      // Only root span should exist
      verifySpans(spans, "root", Map("root" -> Seq("fail")))
    }
  }
}
