package ma.chinespirit.crawldown

import sttp.model.Uri

def run(variant: String, root: String, selector: Option[String]): Unit =
  val rootUri = Uri.parse(root) match
    case Left(error) =>
      println(s"Invalid root URI: $error")
      sys.exit(1)
    case Right(uri) => uri

  val outputDir = os.pwd / Names.toDirname(rootUri)

  variant match
    case "future" =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val fetch = Fetch.future
      val store = Store.future(outputDir)
      val scraper = FutureScraper(fetch, store, rootUri, selector, maxDepth = 20)

      scraper.start()

    case "future-high" =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val fetch = Fetch.future
      val store = Store.future(outputDir)
      val scraper = FutureScraperHighLevel(fetch, store, rootUri, selector, maxDepth = 20)

      scraper.start()

    case "cats" =>
      import cats.effect.unsafe.implicits.global

      val fetch = Fetch.cats
      val store = Store.cats(outputDir)
      val scraper = CatsScraper(fetch, store, rootUri, selector, maxDepth = 20)

      scraper.start.unsafeRunSync()

    case "cats-tf" =>
      import cats.effect.unsafe.implicits.global

      val fetch = Fetch.cats
      val store = Store.cats(outputDir)
      val scraper = CatsScraperTF(fetch, store, rootUri, selector, maxDepth = 20)

      scraper.start.unsafeRunSync()

    case "cats-high" =>
      import cats.effect.unsafe.implicits.global

      val fetch = Fetch.cats
      val store = Store.cats(outputDir)
      val scraper = CatsScraperHighLevel(fetch, store, rootUri, selector, maxDepth = 20)

      scraper.start.unsafeRunSync()

    case "cats-high-tf" =>
      import cats.effect.unsafe.implicits.global

      val fetch = Fetch.cats
      val store = Store.cats(outputDir)
      val scraper = CatsScraperHighLevelTF(fetch, store, rootUri, selector, maxDepth = 20)

      scraper.start.unsafeRunSync()

    case "zio" =>
      val fetch = Fetch.zio
      val store = Store.zio(outputDir)
      val scraper = ZIOScraper(fetch, store, rootUri, selector, maxDepth = 20)

      zio.Unsafe.unsafely {
        zio.Runtime.default.unsafe.run(scraper.start).getOrThrow()
      }

    case "zio-high" =>
      val fetch = Fetch.zio
      val store = Store.zio(outputDir)
      val scraper = ZIOScraperHighLevel(fetch, store, rootUri, selector, maxDepth = 20)

      zio.Unsafe.unsafely {
        zio.Runtime.default.unsafe.run(scraper.start).getOrThrow()
      }

    case "kyo" =>
      import kyo.AllowUnsafe.embrace.danger
      import kyo.*

      val fetch = Fetch.kyo
      val store = Store.kyo(outputDir)
      val tracingScope = Tracing.kyo("kyo-debug")

      KyoApp.Unsafe
        .runAndBlock(Duration.Infinity) {
          Scope.run:
            tracingScope.map: tracing =>
              KyoScraper(fetch, store, tracing, rootUri, selector, maxDepth = 20).start
        }
        .getOrThrow

    case "kyo-high" =>
      import kyo.AllowUnsafe.embrace.danger
      import kyo.*

      val fetch = Fetch.kyo
      val store = Store.kyo(outputDir)
      val scraper = KyoScraperHighLevel(fetch, store, rootUri, selector, maxDepth = 20)

      KyoApp.Unsafe.runAndBlock(Duration.Infinity)(scraper.start).getOrThrow

    case "ox" =>
      val fetch = Fetch.sync
      val store = Store.sync(outputDir)
      val (tracing, stop) = Tracing.sync("ox-debug")
      val scraper = OxScraper(fetch, store, tracing, rootUri, selector, maxDepth = 20)

      try scraper.start()
      finally stop()

    case "ox-high" =>
      val fetch = Fetch.sync
      val store = Store.sync(outputDir)
      val scraper = OxScraperHighLevel(fetch, store, rootUri, selector, maxDepth = 20)

      scraper.start()

    case "gears" =>
      val fetch = Fetch.sync
      val store = Store.sync(outputDir)
      val scraper = GearsScraper(fetch, store, rootUri, selector, maxDepth = 20)

      scraper.start()

    case "gears-high" =>
      val fetch = Fetch.sync
      val store = Store.sync(outputDir)
      val scraper = GearsScraperHighLevel(fetch, store, rootUri, selector, maxDepth = 20)

      scraper.start()

    case _ =>
      println("Usage: crawldown <variant> <root> <selector?>")
      sys.exit(1)

@main def main(args: String*): Unit =
  args.toList match
    case variant :: root :: selector :: Nil =>
      println(s"Running $variant with selector $selector")
      run(variant, root, Some(selector))
    case variant :: root :: Nil =>
      println(s"Running $variant")
      run(variant, root, None)
    case _ =>
      println("Usage: crawldown <variant> <root> <selector?>")
      sys.exit(1)
