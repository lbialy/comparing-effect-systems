package ma.chinespirit.crawldown

import java.util.concurrent.atomic.*
import sttp.model.Uri
import ox.*, either.*, channels.*
import scala.annotation.tailrec

class OxScraperHighLevel(
    fetch: Fetch[Result],
    store: Store[Result],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
):
  private val queue = Channel.unlimited[Scrape]
  private val inFlight = AtomicInteger(0)
  private val visited = AtomicReference[Set[Uri]](Set.empty)

  def start(): Unit =
    inFlight.incrementAndGet()
    queue.send(Scrape(root, 0))

    Vector.fill(parallelism)(()).mapPar(parallelism)(_ => worker(queue, inFlight))

    println("OxScraperHighLevel: Finished.")
  end start

  @tailrec
  private def worker(queue: Channel[Scrape], inFlight: AtomicInteger): Unit =
    queue.receiveOrClosed() match
      case Scrape(uri, depth) =>
        given trace: Trace = Trace(uri)

        if depth >= maxDepth then println(s"$trace: OxScraperHighLevel: depth limit – skipping $uri")
        else if visited.getAndUpdate(_ + uri).contains(uri) then println(s"$trace: OxScraperHighLevel: skipping $uri")
        else crawl(uri, depth).orThrow

        val currentInFlight = inFlight.decrementAndGet()
        if currentInFlight > 0 then worker(queue, inFlight)
        else queue.doneOrClosed()

      case ChannelClosed.Done     => ()
      case ChannelClosed.Error(e) => throw e

  private def crawl(uri: Uri, depth: Int)(using trace: Trace): Result[Unit] = either:
    println(s"$trace: OxScraperHighLevel: crawling $uri")
    val content = fetch.fetch(uri).ok()
    val (links, markdown) = MdConverter.convertAndExtractLinks(content, uri, selector).ok()
    val key = Names.toFilename(uri, root)
    def pushFrontier = links.map(Scrape(_, depth + 1)).foreach { l =>
      inFlight.incrementAndGet()
      queue.send(l)
    }
    def persist = store.store(key, markdown).ok()
    par(pushFrontier, persist)
