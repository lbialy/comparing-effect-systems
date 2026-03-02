package ma.chinespirit.crawldown

import ox.*

import scala.language.experimental.captureChecking

object SafeOx:
  def forkUserSafe[T](f: -> T)(using Ox): Fork[T] = forkUser(f)
