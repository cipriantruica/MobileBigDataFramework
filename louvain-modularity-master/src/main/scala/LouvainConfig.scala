case class LouvainConfig(
  inputFile: String,
  outputDir: String,
  hiveSchema: String,
  hiveInputTable: String,
  parallelism: Int,
  minimumCompressionProgress: Int,
  progressCounter: Int,
  delimiter: String)
