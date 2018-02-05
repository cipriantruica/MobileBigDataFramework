case class LouvainConfig(
  hiveSchema: String,
  hiveInputTable: String,
  hiveInputTableAlpha: String,
  hiveOutputTable: String,
  dateInput: String,
  outputDir: String,
  minimumCompressionProgress: Int,
  progressCounter: Int,
  filter: String)
