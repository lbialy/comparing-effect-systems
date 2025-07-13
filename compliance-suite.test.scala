package ma.chinespirit.crawldown

import munit.FunSuite
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, DurationInt}
import scala.collection.immutable.SortedSet
import scala.collection.immutable.TreeMap

// properties to test
// - 1. X crawler crawls pages in parallel
//      methodology: test server responds with index page immediately, index page contains 8 links,
//      each of them responds after 50ms to provide overlap window, they have no links on them,
//      mock server counts the number of concurrent requests and asserts that it is equal to
//      parallelism of the crawler
// - 2. X no deadlocks: short queue, many links on each page
// - 3. X no cycles: crawler should not crawl the same page twice
// - 4. X concurrent crawls are interrupted when error occurs
//      methodology: index page contains 8 links, 7 of them respond after 100ms, 1 of them fails
//      after 10ms, no calls to md converter should be made
// - 5. X crawler is interrupted when error occurs and errors from worker propagate to top level
// - 6. X max depth is respected
class ComplianceSuite extends FunSuite:
  given ExecutionContext = ExecutionContext.global

  val parallelismRoutes =
    Routes.genTree(depth = 1, linksPerPage = 8).map { case (k, v) => k -> PageSpec(v, if k != "index" then Some(50.millis) else None) }

  val parallelismFixtures = makeScrapers(
    parallelismRoutes,
    scraperParallelism = 8,
    scraperQueueCapacity = 10,
    scrapeMaxDepth = 20,
    futureEC = ExecutionContext.global
  )

  for (name, fixture) <- parallelismFixtures do
    test(s"parallelism - $name") {
      ox.timeout(500.millis)(fixture.run)
      assertEquals(fixture.maxParallelism, 8)
    }

  val cycleRoutes = Map(
    "index" -> PageSpec(Set("a", "b")),
    "a" -> PageSpec(Set("b")),
    "b" -> PageSpec(Set("a"))
  )

  val cycleFixtures = makeScrapers(
    cycleRoutes,
    scraperParallelism = 8,
    scraperQueueCapacity = 10,
    scrapeMaxDepth = 20,
    futureEC = ExecutionContext.global
  )

  for (name, fixture) <- cycleFixtures do
    test(s"cycles handling - $name") {
      ox.timeout(500.millis)(fixture.run)
      assertEquals(fixture.visited.toSet, Set("index", "a", "b"))
      assert(!fixture.hasDuplicates)
    }

  val queueOverloadRoutes = Routes.genTree(depth = 2, linksPerPage = 20).map { case (k, v) => k -> PageSpec(v) }

  val queueOverloadFixtures = makeScrapers(
    queueOverloadRoutes,
    scraperParallelism = 8,
    scraperQueueCapacity = 10,
    scrapeMaxDepth = 20,
    futureEC = ExecutionContext.global
  )

  for (name, fixture) <- queueOverloadFixtures do
    test(s"queue overload - $name") {
      ox.timeout(3000.millis)(fixture.run)
      // depth 2, 20 links per page so a-t and then a/a-t, b/a-t and so on, 421 total
      val expected = SortedSet("index") ++
        ('a' to 't').map(_.toString) ++
        ('a' to 't').flatMap(letter => ('a' to 't').map(letter + "/" + _))

      assertEquals(expected.size, fixture.visited.size)
      assertEquals(fixture.visited.toSet, expected)
    }

  val maxDepthRoutes = Routes.genTree(depth = 10, linksPerPage = 1).map { case (k, v) => k -> PageSpec(v) }

  val maxDepthFixtures = makeScrapers(
    maxDepthRoutes,
    scraperParallelism = 8,
    scraperQueueCapacity = 10,
    scrapeMaxDepth = 5,
    futureEC = ExecutionContext.global
  )

  for (name, fixture) <- maxDepthFixtures do
    test(s"max depth - $name") {
      ox.timeout(1000.millis)(fixture.run)
      // scrapeMaxDepth = 5 - 5 paths in visited set
      assertEquals(fixture.visited.toSet, Set("index", "a", "a/a", "a/a/a", "a/a/a/a"))
    }

  case class TestError(message: String) extends Exception(message)

  val failureRoutes =
    Routes.genTree(depth = 1, linksPerPage = 8).map { case (k, v) =>
      val delay = if k == "h" then Some(10.millis) else if k != "index" then Some(Duration.Inf) else None
      val error = if k == "h" then Some(TestError("test error")) else None

      k -> PageSpec(v, delay, error)
    }

  val failureFixtures = makeScrapers(
    failureRoutes,
    scraperParallelism = 8,
    scraperQueueCapacity = 10,
    scrapeMaxDepth = 20,
    futureEC = ExecutionContext.global
  )

  for (name, fixture) <- failureFixtures do
    test(s"failure handling - $name") {
      intercept[TestError]:
        ox.timeout(3000.millis)(fixture.run)

      assertEquals(fixture.visited.toSet, Set("index", "h"))
    }

end ComplianceSuite
