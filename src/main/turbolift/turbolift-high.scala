package ma.chinespirit.crawldown

import turbolift.effects.IO
import turbolift.io.{Channel, AtomicVar}
import turbolift.!!
import turbolift.bindless._
import turbolift.Extensions._
import sttp.model.Uri

@annotation.nowarn("msg=unused (explicit parameter|pattern variable)") // for `do` macro
final class TurboliftHighScraper(
    fetch: Fetch[TTask],
    store: Store[TTask],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
):
  def start: Unit !! IO =
    `do`:
      val queue = Channel.unbounded[Scrape | Done].!
      val visited = AtomicVar.createLockless(Set[Uri]()).!
      val inFlight = AtomicVar.createLockless(0).!

      inFlight.modify(_ + 1).!
      queue.put(Scrape(root, 0)).!
      (1 to parallelism).foreachParEff(_ => worker(queue, visited, inFlight)).!

  private def worker(queue: Channel[Scrape | Done], visited: AtomicVar[Set[Uri]], inFlight: AtomicVar[Int]): Unit !! IO =
    `do`:
      queue.get.! match
        case Done => ()
        case Scrape(uri, depth) =>
          if depth >= maxDepth then ()
          else if visited.getModify(_ + uri).!.contains(uri) then ()
          else crawl(uri, depth, queue, inFlight).!

          val currentInFlight = inFlight.modifyGet(_ - 1).!
          if currentInFlight > 0 then worker(queue, visited, inFlight).!
          else (1 to parallelism).foreachEff(_ => queue.put(Done)).!

  private def crawl(uri: Uri, depth: Int, queue: Channel[Scrape | Done], inFlight: AtomicVar[Int]): Unit !! IO =
    `do`:
      val content = fetch.fetch(uri).!
      val (links, markdown) = IO.raiseFromEither(MdConverter.convertAndExtractLinks(content, uri, selector)).!
      val pushFrontier = links.foreachEff(uri => inFlight.modify(_ + 1) &&! queue.put(Scrape(uri, depth + 1)))
      val persist = store.store(Names.toFilename(uri, root), markdown)
      (pushFrontier &! persist).!
