export interface DataQualityRulesetParams {
  tableName: string
  rulesetName: string
  rules: string
}

const titleBasicsDataQualityRulesets: DataQualityRulesetParams = {
  tableName: 'title_basics',
  rulesetName: 'titleBasicsDataQualityRuleset',
  rules: `
  Rules = [
          IsComplete "tconst",
          CustomSql "SELECT COUNT(*) FROM (SELECT year, month, day, tconst FROM imdb_raw_datasets.title_basics GROUP BY year, month, day, tconst HAVING COUNT(*) > 1)" = 0
          ]
  `
}

export const rawDatasetsDataQualityParameters: DataQualityRulesetParams[] = [titleBasicsDataQualityRulesets]
