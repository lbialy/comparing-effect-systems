//> using scala "3.8.0-RC1-bin-20250819-1f13619-NIGHTLY"
//> using repository https://repo.scala-lang.org/artifactory/maven-nightlies/

// scala 3 compiler options

///> using option "-Wvalue-discard"
///> using option "-Wnonunit-statement"
///> using option "-Wconf:msg=(unused.*value|discarded.*value|pure.*statement):error"
///> using option "-language:strictEquality"

///> using option -language:experimental.captureChecking
///> using option -Xprint:cc

//> using option -Wunused:all
//> using option -deprecation

// crawler dependencies

//> using dep com.softwaremill.sttp.client4::core:4.0.9
//> using dep com.vladsch.flexmark:flexmark-html2md-converter:0.64.8
//> using dep com.lihaoyi::os-lib:0.11.4

// effect / direct-style dependencies

//> using dep org.typelevel::cats-effect:3.6.3

//> using dep dev.zio::zio:2.1.20

//> using dep com.softwaremill.ox::core:1.0.0

//> using dep io.getkyo::kyo-core:1.0-RC1
//> using dep io.getkyo::kyo-combinators:1.0-RC1

//> using dep ch.epfl.lamp::gears:0.0.0+317-f49657d8-SNAPSHOT
//> using dep ch.epfl.lamp::steps:0.1.0-RC1+2-aac846d4-SNAPSHOT

// tracing and testing dependencies

//> using dep ch.qos.logback:logback-classic:1.5.18

//> using dep com.github.plokhotnyuk.jsoniter-scala::jsoniter-scala-core:2.36.7
//> using dep com.github.plokhotnyuk.jsoniter-scala::jsoniter-scala-macros:2.36.7

//> using test.dep com.softwaremill.sttp.tapir::tapir-files:1.11.37
//> using test.dep com.softwaremill.sttp.tapir::tapir-jdkhttp-server:1.11.37

//> using test.dep org.scalameta::munit:1.1.1
