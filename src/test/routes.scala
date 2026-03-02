package ma.chinespirit.crawldown

import scala.collection.immutable.SortedSet
import munit.FunSuite
import scala.collection.immutable.TreeMap

object Routes:
  def genTree(depth: Int, linksPerPage: Int): Map[String, Set[String]] =
    def toExcelColumn(n: Int): String =
      if n < 26 then ('a' + n).toChar.toString
      else toExcelColumn((n / 26) - 1) + ('a' + (n % 26)).toChar.toString

    def generateLinks(currentDepth: Int, currentPath: String): Set[String] =
      if currentDepth >= depth then Set.empty
      else
        (0 until linksPerPage)
          .map(i =>
            if currentPath == "index" then toExcelColumn(i)
            else s"$currentPath/${toExcelColumn(i)}"
          )
          .to(SortedSet)

    def generatePages(currentDepth: Int, currentPath: String): Map[String, Set[String]] =
      if currentDepth > depth then Map.empty
      else
        val links = generateLinks(currentDepth, currentPath)
        val currentPage = Map(currentPath -> links)
        val childPages = links.flatMap(link => generatePages(currentDepth + 1, link)).toMap
        currentPage ++ childPages

    generatePages(0, "index")
  end genTree

class RoutesSuite extends FunSuite:
  test("genTree with depth 0 and maxLinks 2") {
    val result = Routes.genTree(0, 2)
    val expected = Map("index" -> Set.empty[String])
    assertEquals(result, expected)
  }

  test("genTree with depth 1 and maxLinks 2") {
    val result = Routes.genTree(1, 2)
    val expected = Map(
      "index" -> Set("a", "b"),
      "a" -> Set.empty[String],
      "b" -> Set.empty[String]
    )
    assertEquals(result, expected)
  }

  test("genTree with depth 2 and maxLinks 2") {
    val result = Routes.genTree(2, 2)
    val expected = Map(
      "index" -> Set("a", "b"),
      "a" -> Set("a/a", "a/b"),
      "b" -> Set("b/a", "b/b"),
      "a/a" -> Set.empty[String],
      "a/b" -> Set.empty[String],
      "b/a" -> Set.empty[String],
      "b/b" -> Set.empty[String]
    )
    assertEquals(result, expected)
  }

  test("genTree with depth 1 and maxLinks 3") {
    val result = Routes.genTree(1, 3)
    val expected = Map(
      "index" -> Set("a", "b", "c"),
      "a" -> Set.empty[String],
      "b" -> Set.empty[String],
      "c" -> Set.empty[String]
    )
    assertEquals(result, expected)
  }

  test("genTree with depth 2 and maxLinks 1") {
    val result = Routes.genTree(2, 1)
    val expected = Map(
      "index" -> Set("a"),
      "a" -> Set("a/a"),
      "a/a" -> Set.empty[String]
    )
    assertEquals(result, expected)
  }

  test("genTree handles Excel column naming correctly") {
    val result = Routes.genTree(1, 27) // This will test a, b, ..., z, aa
    val indexLinks = result("index")

    // Check that we have the expected number of links
    assertEquals(indexLinks.size, 27)

    // Check specific expected values
    assert(indexLinks.contains("a"))
    assert(indexLinks.contains("z"))
    assert(indexLinks.contains("aa"))

    // Check that all links are valid Excel column names
    val validPattern = "^[a-z]+$".r
    indexLinks.foreach { link =>
      assert(validPattern.matches(link), s"Link '$link' is not a valid Excel column name")
    }
  }

  test("genTree with larger depth and links") {
    val result = Routes.genTree(3, 2)

    // Check that index has correct links
    assertEquals(result("index"), Set("a", "b"))

    // Check that depth 1 pages have correct links
    assertEquals(result("a"), Set("a/a", "a/b"))
    assertEquals(result("b"), Set("b/a", "b/b"))

    // Check that depth 2 pages have correct links
    assertEquals(result("a/a"), Set("a/a/a", "a/a/b"))
    assertEquals(result("a/b"), Set("a/b/a", "a/b/b"))
    assertEquals(result("b/a"), Set("b/a/a", "b/a/b"))
    assertEquals(result("b/b"), Set("b/b/a", "b/b/b"))

    // Check that depth 3 pages have no links
    assertEquals(result("a/a/a"), Set.empty[String])
    assertEquals(result("a/a/b"), Set.empty[String])
    assertEquals(result("a/b/a"), Set.empty[String])
    assertEquals(result("a/b/b"), Set.empty[String])
    assertEquals(result("b/a/a"), Set.empty[String])
    assertEquals(result("b/a/b"), Set.empty[String])
    assertEquals(result("b/b/a"), Set.empty[String])
    assertEquals(result("b/b/b"), Set.empty[String])

    // Check total number of pages
    assertEquals(result.size, 15) // 1 + 2 + 4 + 8 = 15 pages
  }

  test("genTree with depth 10 and linksPerPage 1") {
    val result = Routes.genTree(10, 1) -- Set("index")
    result.to(TreeMap).zipWithIndex.foreach { case ((k, _), i) =>
      val computedRoute = Vector.fill(i + 1)("a").mkString("/")
      assertEquals(k, computedRoute)
    }
    assertEquals(result.size, 10)
  }
