package ma.chinespirit.crawldown

import turbolift.effects.{IO, LazyMemoizerEffect, ReaderEffect}
import turbolift.io.Semaphore
import turbolift.!!
import turbolift.bindless._
import turbolift.Extensions._
import sttp.model.Uri

@annotation.nowarn("msg=unused (explicit parameter|pattern variable)") // for `do` macro
final class TurboliftMemoScraper(
    fetch: Fetch[TTask],
    store: Store[TTask],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
):
  def start: Unit !! IO =
    `do`:
      val semaphore = Semaphore.create(parallelism).!
      crawl(root)
        .handleWith(Memo.handlers.shared(crawl_impl(semaphore)))
        .handleWith(Depth.handler(0))
        .!

  // `LazyMemoizer[Key, Value]` is typically used with data types other than `Unit`, to build cyclic data
  // structures (e.g. graphs). For `crawl`, it just ensures each `uri`s is visited at most once.
  private case object Memo extends LazyMemoizerEffect[Uri, Unit]
  private case object Depth extends ReaderEffect[Int]
  private type Memo = Memo.type
  private type Depth = Depth.type

  private def crawl(uri: Uri): Unit !! Memo = Memo.memo(uri).as(())

  private def crawl_impl(semaphore: Semaphore)(uri: Uri): Unit !! (IO & Memo & Depth) =
    semaphore
      .use(1):
        `do`:
          if Depth.ask.! < maxDepth then
            val content = fetch.fetch(uri).!
            val (links, markdown) = IO.raiseFromEither(MdConverter.convertAndExtractLinks(content, uri, selector)).!
            val pushFrontier =
              Depth.localModify(_ + 1):
                links.foreachParEff(crawl)
            val fiber = pushFrontier.fork.!
            store.store(Names.toFilename(uri, root), markdown).!
            // Waiting for `pushFrontier` completion needs to be moved outside of
            // the semaphore's scope, because `Memo.memo(uri)` is synchronous.
            fiber.join
          else !!.unit
      .flatten
      .warp // This won't be necessary with the next Turbolift's release.
