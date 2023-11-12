import { Stack, type StackProps, RemovalPolicy } from 'aws-cdk-lib'
import { type Construct } from 'constructs'
import * as s3 from 'aws-cdk-lib/aws-s3'
import * as glue from 'aws-cdk-lib/aws-glue'
import * as s3_deployment from 'aws-cdk-lib/aws-s3-deployment'
import * as data_quality_constants from './data_quality_constants'
import { type glueTables } from '../data_catalog_stack'

interface DataQualityStackProps extends StackProps {
  glueDatabaseName: string
  glueTables: glueTables
  glueIAMRoleArn: string
}

type DataQualityRulesets = Record<string, glue.CfnDataQualityRuleset>

export class DataQualityStack extends Stack {
  private readonly glueJobS3Bucket: s3.IBucket
  private readonly glueScriptDeployment: s3_deployment.BucketDeployment
  private readonly glueDataQualityRulesets: DataQualityRulesets
  private readonly dataQualityGlueJob: glue.CfnJob

  constructor (scope: Construct, id: string, props: DataQualityStackProps) {
    super(scope, id, props)
    this.glueJobS3Bucket = new s3.Bucket(
      this,
      id = 'dataQualityGlueJobBucket',
      {
        removalPolicy: RemovalPolicy.DESTROY,
        autoDeleteObjects: true
      })

    this.glueScriptDeployment = new s3_deployment.BucketDeployment(
      this,
      'dataQualityGlueJobS3Deployment',
      {
        sources: [s3_deployment.Source.asset('./lib/data_quality/glue_scripts/')],
        destinationBucket: this.glueJobS3Bucket
      }
    )

    this.glueScriptDeployment.node.addDependency(this.glueJobS3Bucket)

    this.glueDataQualityRulesets = this.createDataQualityRulesets(props.glueDatabaseName, props.glueTables)

    this.dataQualityGlueJob = this.createDataQualityGlueJob(props.glueIAMRoleArn)
  }

  private createDataQualityRulesets (glueDatabaseName: string, glueTables: glueTables): DataQualityRulesets {
    const dataQualityRulesets: DataQualityRulesets = {}

    data_quality_constants.rawDatasetsDataQualityParameters.forEach(dataQualityParam => {
      dataQualityRulesets[dataQualityParam.rulesetName] = new glue.CfnDataQualityRuleset(
        this,
        dataQualityParam.rulesetName,
        {
          name: dataQualityParam.rulesetName,
          targetTable: { databaseName: glueDatabaseName, tableName: dataQualityParam.tableName },
          ruleset: dataQualityParam.rules
        }
      )

      dataQualityRulesets[dataQualityParam.rulesetName].node.addDependency(glueTables[dataQualityParam.tableName])
    })

    return dataQualityRulesets
  }

  private createDataQualityGlueJob (glueIAMRole: string): glue.CfnJob {
    const glueJob = new glue.CfnJob(
      this,
      'dataQualityGlueJob',
      {
        name: 'dataQualityGlueJob',
        command: {
          name: 'pythonshell',
          pythonVersion: '3.9',
          scriptLocation: `s3://${this.glueJobS3Bucket.bucketName}/data_quality_glue_job.py`
        },
        role: glueIAMRole,
        glueVersion: '3.0',
        defaultArguments: { '--additional-python-modules': 'boto3==1.28.84' }
      }
    )

    glueJob.node.addDependency(this.glueJobS3Bucket)
    glueJob.node.addDependency(this.glueScriptDeployment)

    return glueJob
  }
}
