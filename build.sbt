lazy val root = project
  .in(file("."))
  .settings(
    name                                    := "RChain dev finalization",
    version                                 := "0.1.0-SNAPSHOT",
    scalaVersion                            := "2.12.11",
    scalafmtOnCompile                       := true,
    libraryDependencies += "org.typelevel"  %% "cats-core"   % "2.7.0",
    libraryDependencies += "org.typelevel"  %% "cats-effect" % "2.5.4",
    libraryDependencies += "io.monix"       %% "monix"       % "3.4.0",
    libraryDependencies += "org.scalactic"  %% "scalactic"   % "3.0.5" % "test",
    libraryDependencies += "org.scalatest"  %% "scalatest"   % "3.0.5" % "test",
    libraryDependencies += "org.scalacheck" %% "scalacheck"  % "1.15.2",
    scalacOptions ++= Seq(
      // To silence warning when defining Scala 2 extensions - implicit def ...
      "-language:implicitConversions"
    )
  )
