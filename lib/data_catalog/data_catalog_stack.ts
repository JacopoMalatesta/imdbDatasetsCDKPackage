import { Stack, type StackProps, Aws } from 'aws-cdk-lib'
import { type Construct } from 'constructs'
import * as sqs from 'aws-cdk-lib/aws-sqs'
import * as glue from 'aws-cdk-lib/aws-glue'
import * as iam from 'aws-cdk-lib/aws-iam'
import * as events from 'aws-cdk-lib/aws-events'
import type * as sns from 'aws-cdk-lib/aws-sns'
import * as sns_subscriptions from 'aws-cdk-lib/aws-sns-subscriptions'

export type glueTables = Record<string, glue.CfnTable>

const glueTablesNames: string[] = ['title_basics', 'title_ratings', 'title_principals', 'name_basics']

interface DataCatalogStackProps extends StackProps {
  snsTopic: sns.ITopic
}

export class DataCatalogStack extends Stack {
  private readonly sqsQueue: sqs.IQueue
  private readonly glueDatabase: glue.CfnDatabase
  public readonly glueTables: glueTables
  private readonly glueWorkflow: glue.CfnWorkflow
  private readonly glueRole: iam.Role
  private readonly glueCrawler: glue.CfnCrawler
  private readonly crawlerTrigger: glue.CfnTrigger
  private readonly eventRuleRole: iam.IRole
  private readonly eventBridgeRule: events.CfnRule

  constructor (scope: Construct, id: string, props?: DataCatalogStackProps) {
    super(scope, id, props)

    this.sqsQueue = new sqs.Queue(this, 'imdb-data-manipulation-queue')
    props?.snsTopic.addSubscription(new sns_subscriptions.SqsSubscription(this.sqsQueue))
    this.glueDatabase = this.createGlueDatabase()
    this.glueWorkflow = new glue.CfnWorkflow(this, 'imdb-data-manipulation-workflow', {
      name: 'imdb-data-manipulation-workflow'
    })
    this.glueRole = this.createGlueRole()
    this.glueTables = this.createGlueTables()
    this.glueCrawler = this.createGlueCrawler(this.glueRole.roleArn, this.sqsQueue.queueArn)
    this.crawlerTrigger = this.createCrawlerTrigger(this.glueCrawler.name, this.glueWorkflow.name)
    this.eventRuleRole = this.createRoleForEventBridgeRule()
    this.eventBridgeRule = this.createEventBridgeRule(this.eventRuleRole.roleArn)
  }

  private createGlueDatabase (): glue.CfnDatabase {
    return new glue.CfnDatabase(this, 'imdb_raw_datasets', {
      catalogId: Aws.ACCOUNT_ID,
      databaseInput: { name: 'imdb_raw_datasets' }
    })
  }

  private createGlueRole (): iam.Role {
    const permissions: string[] = ['s3:*', 'glue:*', 'iam:*', 'logs:*',
      'cloudwatch:*', 'sqs:*', 'ec2:*', 'cloudtrail:*']
    const PolicyStatement: iam.PolicyStatement = new iam.PolicyStatement(
      { effect: iam.Effect.ALLOW, actions: permissions, resources: ['*'] })

    return new iam.Role(this,
      'imdb-glue-role',
      {
        roleName: 'imdbGlueRole',
        assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
        inlinePolicies: { gluePolicy: new iam.PolicyDocument({ statements: [PolicyStatement] }) }
      })
  }

  private createGlueTables (): glueTables {
    const glueTables: glueTables = {}

    for (const tableName of glueTablesNames) {
      glueTables[tableName] = new glue.CfnTable(
        this,
        tableName,
        {
          databaseName: 'imdb_raw_datasets',
          catalogId: Aws.ACCOUNT_ID,
          tableInput: {
            name: tableName,
            parameters: {
              classification: 'csv',
              compressionType: 'gzip',
              'skip.header.line.count': 1
            },
            partitionKeys: [
              { name: 'year', type: 'int' },
              { name: 'month', type: 'int' },
              { name: 'day', type: 'int' }
            ],
            storageDescriptor: {
              location: `s3://imdb-dataset-raw-zone/imdb_raw_datasets/${tableName.replace('_', '.')}`,
              compressed: true,
              inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
              outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
              serdeInfo: {
                name: 'imdb-raw-datasets-serde',
                serializationLibrary: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                parameters: { 'field.delim': '\t' }
              }
            }
          }
        }
      )

      glueTables[tableName].node.addDependency(this.glueDatabase)
    }

    return glueTables
  }

  private createGlueCrawler (iamRoleArn: string, sqsQueueArn: string): glue.CfnCrawler {
    const crawlerConfiguration: string = `{
      "Version": 1.0,
      "CrawlerOutput": {
        "Tables": {
          "AddOrUpdateBehavior": "MergeNewColumns",
          "TableThreshold": 4
        }
      }
    }`

    const crawler = new glue.CfnCrawler(
      this,
      'IMDBRawDatasetsCrawler',
      {
        name: 'IMDBRawDatasetsCrawler',
        role: iamRoleArn,
        databaseName: 'imdb_raw_datasets',
        targets: {
          catalogTargets: [{
            databaseName: 'imdb_raw_datasets',
            eventQueueArn: sqsQueueArn,
            tables: glueTablesNames
          }]
        },
        recrawlPolicy: { recrawlBehavior: 'CRAWL_EVENT_MODE' },
        schemaChangePolicy: { deleteBehavior: 'LOG' },
        configuration: crawlerConfiguration
      }
    )

    for (const key in this.glueTables) {
      const table = this.glueTables[key]
      crawler.node.addDependency(table)
    }

    return crawler
  }

  private createCrawlerTrigger (glueCrawlerName: string | undefined, glueWorkflowName: string | undefined): glue.CfnTrigger {
    const crawlerTrigger = new glue.CfnTrigger(
      this,
      'glueCrawlerTrigger',
      {
        name: 'GlueCrawlerTrigger',
        actions: [{
          crawlerName: glueCrawlerName,
          timeout: 5
        }],
        type: 'EVENT',
        workflowName: glueWorkflowName,
        eventBatchingCondition: { batchSize: glueTablesNames.length }
      }
    )

    crawlerTrigger.node.addDependency(this.glueWorkflow)
    crawlerTrigger.node.addDependency(this.glueCrawler)

    return crawlerTrigger
  }

  private createRoleForEventBridgeRule (): iam.IRole {
    return new iam.Role(
      this,
      'eventBridgeRuleRole',
      {
        roleName: 'eventBridgeRuleRole',
        assumedBy: new iam.ServicePrincipal('events.amazonaws.com'),
        inlinePolicies: {
          eventBridgePolicy: new iam.PolicyDocument(
            {
              statements: [new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: ['events:*', 'glue:*'],
                resources: ['*']
              })]
            }
          )
        }
      }
    )
  }

  private createEventBridgeRule (eventBridgeRoleArn: string): events.CfnRule {
    const rule = new events.CfnRule(
      this,
      'glueWorkflowEventBridgeRule',
      {
        roleArn: eventBridgeRoleArn,
        targets: [{
          id: Aws.ACCOUNT_ID,
          arn: `arn:aws:glue:${Aws.REGION}:${Aws.ACCOUNT_ID}:workflow/imdb-data-manipulation-workflow`,
          roleArn: eventBridgeRoleArn
        }],
        eventPattern: {
          source: ['aws.s3'],
          'detail-type': ['Object Created'],
          detail: {
            bucket: { name: ['imdb-dataset-raw-zone'] },
            object: { key: [{ prefix: 'imdb_raw_datasets' }] }
          }
        }
      }
    )

    rule.node.addDependency(this.glueWorkflow)

    return rule
  }

  public getGlueDatabaseName (): string {
    return 'imdb_raw_datasets'
  }

  public getGlueIAMRoleArn (): string {
    return this.glueRole.roleArn
  }
}
