package ma.chinespirit.crawldown

import java.util.concurrent.atomic.*
import sttp.model.Uri
import ox.*, either.*, channels.*
import scala.annotation.tailrec

class OxScraperHighLevel(
    fetch: Fetch[EitherErrorOr],
    store: Store[EitherErrorOr],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
):
  type Visited = AtomicReference[Set[Uri]]

  def start(): Unit =
    val queue = Channel.unlimited[Scrape]
    val inFlight = AtomicInteger(0)
    val visited = AtomicReference[Set[Uri]](Set.empty)

    inFlight.incrementAndGet()
    queue.send(Scrape(root, 0))

    Vector.fill(parallelism)(()).mapPar(parallelism)(_ => worker(queue, visited, inFlight))
  end start

  @tailrec
  private def worker(queue: Channel[Scrape], visited: Visited, inFlight: AtomicInteger): Unit =
    queue.receiveOrClosed() match
      case Scrape(uri, depth) =>
        if depth >= maxDepth then ()
        else if visited.getAndUpdate(_ + uri).contains(uri) then ()
        else crawl(uri, depth, queue, inFlight).orThrow

        val currentInFlight = inFlight.decrementAndGet()
        if currentInFlight > 0 then worker(queue, visited, inFlight)
        else queue.doneOrClosed()

      case ChannelClosed.Done     => ()
      case ChannelClosed.Error(e) => throw e

  private def crawl(uri: Uri, depth: Int, queue: Channel[Scrape], inFlight: AtomicInteger): Either[Throwable, Unit] = either:
    val content = fetch.fetch(uri).ok()
    val (links, markdown) = MdConverter.convertAndExtractLinks(content, uri, selector).ok()
    val key = Names.toFilename(uri, root)
    def pushFrontier = links.map(Scrape(_, depth + 1)).foreach { l =>
      inFlight.incrementAndGet()
      queue.send(l)
    }
    def persist = store.store(key, markdown).ok()
    par(pushFrontier, persist)
