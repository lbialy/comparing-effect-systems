package ma.chinespirit.crawldown

import scala.language.experimental.captureChecking

import gears.async.{Future => GearsFuture, *}

/** We need to define a capture checker aware version of Gears' Future.apply. This is a bit of a hack because we disallow capturing of any
  * capabilites by the Future but the limitations of the current implementation (and my lack of skills in this area) make it necessary.
  */
object Future:
  // with gears 0.0.0+317-f49657d8-SNAPSHOT from github.com/natsukagami/async at add-cc branch (f49657d8c78bef4b2689837b4415947aac752a91)
  // this requires ^{asyncSpawn} capability capture annotation, on gears 0.2.0 it doesn't (it's not capchecked)
  def apply[T](body: Async.Spawn ?-> T)(using asyncSpawn: Async.Spawn): GearsFuture[T] = // ^{asyncSpawn} =
    GearsFuture(body)
