case class LouvainConfig(
  hiveSchema: String,
  hiveInputTable: String,
  hiveInputTableAlpha: String,
  hiveOutputTable: String,
  dateInput: String,
  alphaThreshold: String,
  edgeCostFactor: String,
  minimumCompressionProgress: Int,
  progressCounter: Int)
