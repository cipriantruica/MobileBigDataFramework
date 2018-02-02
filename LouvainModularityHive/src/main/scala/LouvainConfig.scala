case class LouvainConfig(
  hiveSchema: String,
  hiveInputTable: String,
  dateInput: String,
  outputDir: String,
  minimumCompressionProgress: Int,
  progressCounter: Int)
