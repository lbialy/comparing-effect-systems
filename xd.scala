// def catsTF[F[_]: Sync](uri: Uri): Fetch[F] = new Fetch[F]:
//   def fetch(uri: Uri): F[String] =
//     Sync[F].blocking(basicRequest.get(uri).send()).flatMap { response =>
//       response.body match
//         case Left(error) =>
//           Sync[F].raiseError(Exception(s"Failed to fetch $uri: $error"))
//         case Right(body) => Sync[F].pure(body)
//     }

// def catsTF[F[_]: Sync](dir: os.Path): Store[F] =
//   os.makeDir.all(dir)
//   new Store[F]:
//     def store(key: String, value: String): F[Unit] =
//       Sync[F].blocking(os.write(dir / key, value))

// def makeCatsScraperTF: ScraperAdapter =
//   val services = makeCatsServicesTF[IO]
//   val scraper = CatsScraperTF(
//     fetch = services,
//     store = services,
//     root = uri"http://localhost:8080/",
//     selector = None,
//     maxDepth = scrapeMaxDepth,
//     parallelism = scraperParallelism,
//     queueCapacity = scraperQueueCapacity
//   )

//   new ScraperAdapter:
//     def maxParallelism: Int = services.maxParallelism
//     def visited: Vector[String] = services.visited
//     def hasDuplicates: Boolean = services.hasDuplicates
//     def run: Unit =
//       import cats.effect.unsafe.implicits.global
//       scraper.start.unsafeRunSync()

// def makeCatsScraperHighTF: ScraperAdapter =
//   val services = makeCatsServicesTF[IO]
//   val scraper = CatsScraperHighLevelTF(
//     fetch = services,
//     store = services,
//     root = uri"http://localhost:8080/",
//     selector = None,
//     maxDepth = scrapeMaxDepth,
//     parallelism = scraperParallelism
//   )

//   new ScraperAdapter:
//     def maxParallelism: Int = services.maxParallelism
//     def visited: Vector[String] = services.visited
//     def hasDuplicates: Boolean = services.hasDuplicates
//     def run: Unit =
//       import cats.effect.unsafe.implicits.global
//       scraper.start.unsafeRunSync()

// Map(
//   "future" -> makeFutureScraper,
//   "cats" -> makeCatsScraper,
//   "zio" -> makeZioScraper,
//   "kyo" -> makeKyoScraper,
//   "ox" -> makeOxScraper,
//   "gears" -> makeGearsScraper,
//   "cats-tf" -> makeCatsScraperTF,
//   "future-high-level" -> makeFutureScraperHighLevel,
//   "cats-high-level" -> makeCatsScraperHighLevel,
//   "zio-high-level" -> makeZioScraperHighLevel,
//   "kyo-high-level" -> makeKyoScraperHighLevel,
//   "ox-high-level" -> makeOxScraperHighLevel,
//   "gears-high-level" -> makeGearsScraperHighLevel,
//   "cats-high-tf" -> makeCatsScraperHighTF
// )
