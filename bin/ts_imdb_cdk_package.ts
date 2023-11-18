#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib'
import { DataExtractionStack } from '../lib/data_extraction/data_extraction_stack'
import { DataCatalogStack } from '../lib/data_catalog/data_catalog_stack'
import { DataQualityStack } from '../lib/data_quality/data_quality_stack'

const euWestOneEnvironment = { account: '925000840053', region: 'eu-west-1' }

const app = new cdk.App()
const dataExtractionStack = new DataExtractionStack(app, 'DataExtractionStack', { env: euWestOneEnvironment })
const dataCatalogStack = new DataCatalogStack(app, 'DataCatalogStack', { env: euWestOneEnvironment, snsTopic: dataExtractionStack.snsTopic })
const dataQualityStack = new DataQualityStack(app, 'DataQualityStack', {
  env: euWestOneEnvironment,
  glueDatabaseName: dataCatalogStack.getGlueDatabaseName(),
  glueTables: dataCatalogStack.glueTables,
  glueIAMRoleArn: dataCatalogStack.getGlueIAMRoleArn(),
  glueCrawlerName: dataCatalogStack.getCrawlerName(),
  glueWorkflowName: dataCatalogStack.getWorkflowName()
})
