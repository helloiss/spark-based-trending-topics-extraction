package nl.svanwouw.trending


object Main {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: Main <host> <input_file> <output_dir>")
      System.exit(1)
    }
    TwitterWordFrequency.execute(args(0), args(1), args(2))
  }
}