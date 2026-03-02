package ma.chinespirit.crawldown

import sttp.model.Uri
import com.vladsch.flexmark.util.data.MutableDataSet
import com.vladsch.flexmark.html2md.converter.*
import scala.jdk.CollectionConverters.*
import org.jsoup.Jsoup

object MdConverter:

  private val Converter = FlexmarkHtmlConverter.builder(MutableDataSet()).build()

  private val linkTags = Vector(
    "a[href]" -> "href",
    "iframe[src]" -> "src",
    "source[src]" -> "src"
  )

  def convertAndExtractLinks(content: String, baseUri: Uri, selector: Option[String]): Either[Exception, (Vector[Uri], String)] =
    try
      val host = baseUri.host.getOrElse {
        throw RuntimeException(s"No host found in base URI: $baseUri")
      }

      val parsed = Jsoup.parse(content, baseUri.toString)
      val uris = for
        (tag, attr) <- linkTags
        node <- parsed.select(tag).asScala
        absUrl = node.absUrl(attr) if absUrl.nonEmpty
      yield Uri.parse(absUrl).getOrElse {
        throw RuntimeException(s"Invalid link: $absUrl")
      }

      val selfLinks = uris.filter(_.host.contains(host))
      val noFragmentLinks = selfLinks.map(_.fragment(None)).distinct

      val htmlNodeToConvert = selector match
        case Some(selector) => parsed.select(selector).first()
        case None           => parsed

      val markdown = Converter.convert(htmlNodeToConvert)

      Right((noFragmentLinks, markdown))
    catch
      case ex: Exception =>
        Left(ex)
