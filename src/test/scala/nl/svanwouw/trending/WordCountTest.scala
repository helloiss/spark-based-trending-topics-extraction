package nl.svanwouw.trending

import java.io.{File, FileWriter}

import com.google.common.io.Files
import org.specs2.mutable.SpecificationWithJUnit

import scala.io.Source

class WordCountTest extends SpecificationWithJUnit {

  "A WordCount job" should {

    "count words correctly" in {

      val tempDir = Files.createTempDir()
      val inputFile = new File(tempDir, "input").getAbsolutePath
      val inWriter = new FileWriter(inputFile)
      inWriter.write("hack hack hack and hack")
      inWriter.close()
      val outputDir = new File(tempDir, "output").getAbsolutePath

      WordCount.execute(
        master = "local",
        args   = List(inputFile, outputDir)
      )

      val outputFile = new File(outputDir, "part-00000")
      val actual = Source.fromFile(outputFile).mkString
      actual must_== "(hack,4)\n(and,1)\n"
    }
  }
}

