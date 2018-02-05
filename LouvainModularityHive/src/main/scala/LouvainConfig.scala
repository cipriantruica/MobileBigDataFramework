case class LouvainConfig(
  hiveSchema: String,
  hiveInputTable: String,
  hiveOutputTable: String,
  dateInput: String,
  outputDir: String,
  minimumCompressionProgress: Int,
  progressCounter: Int)
