//> using scala 3.8.2

// scala 3 compiler options - disabled because different solutions prefer different options

///> using option "-Werror"
///> using option "-Wvalue-discard"
///> using option "-Wnonunit-statement"
///> using option "-Wconf:msg=(unused.*value|discarded.*value|pure.*statement):error"
///> using option "-language:strictEquality"

//> using option -Wunused:all
//> using option -deprecation

// crawler dependencies

//> using dep com.softwaremill.sttp.client4::core:4.0.19
//> using dep com.vladsch.flexmark:flexmark-html2md-converter:0.64.8
//> using dep com.lihaoyi::os-lib:0.11.8

// effect / direct-style dependencies

//> using dep org.typelevel::cats-effect:3.6.3

//> using dep dev.zio::zio:2.1.24

//> using dep com.softwaremill.ox::core:1.0.4

//> using dep io.getkyo::kyo-core:1.0-RC1
//> using dep io.getkyo::kyo-combinators:1.0-RC1

//> using dep ch.epfl.lamp::gears:0.2.0
//> using dep ch.epfl.lamp::steps:0.2.0

// tracing and testing dependencies

//> using dep ch.qos.logback:logback-classic:1.5.32

//> using dep com.github.plokhotnyuk.jsoniter-scala::jsoniter-scala-core:2.38.9
//> using dep com.github.plokhotnyuk.jsoniter-scala::jsoniter-scala-macros:2.38.9

//> using test.dep com.softwaremill.sttp.tapir::tapir-files:1.13.9
//> using test.dep com.softwaremill.sttp.tapir::tapir-jdkhttp-server:1.13.9

//> using test.dep org.scalameta::munit:1.2.3
