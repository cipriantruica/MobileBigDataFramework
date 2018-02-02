case class LouvainConfig(
  inputFile: String,
  outputDir: String,
  hiveSchema: String,
  hiveInputTable: String,
  dateInput: String
  minimumCompressionProgress: Int,
  progressCounter: Int
