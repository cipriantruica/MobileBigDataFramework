case class LouvainConfig(
                          hiveSchema: String,
                          hiveInputTable: String,
                          hiveInputTableAlpha: String,
                          hiveInputTableEdgesAlpha: String,
                          hiveOutputTable: String,
                          noTables: Int,
                          dateInput: String,
                          alphaThreshold: String,
                          edgeCostFactor: String,
                          minimumCompressionProgress: Int,
                          progressCounter: Int)
