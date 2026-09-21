import { createHash } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";

const root = path.resolve("dashboard");
const projectName = "BNPL Big Data Report";
const reportDir = path.join(root, `${projectName}.Report`);
const modelDir = path.join(root, `${projectName}.SemanticModel`);
const definitionDir = path.join(reportDir, "definition");

for (const target of [reportDir, modelDir]) {
  if (!target.startsWith(`${root}${path.sep}`)) {
    throw new Error(`Refusing to replace a directory outside ${root}: ${target}`);
  }
}

const json = (value) => `${JSON.stringify(value, null, 2)}\n`;
const id = (value, length = 20) => createHash("sha256").update(value).digest("hex").slice(0, length);
const guid = (value) => {
  const hex = id(value, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};
const write = async (file, value) => {
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, typeof value === "string" ? value : json(value), "utf8");
};

await rm(reportDir, { recursive: true, force: true });
await rm(modelDir, { recursive: true, force: true });

await write(path.join(root, `${projectName}.pbip`), {
  $schema: "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
  version: "1.0",
  artifacts: [{ report: { path: `${projectName}.Report` } }],
  settings: { enableAutoRecovery: true },
});

await write(path.join(modelDir, "definition.pbism"), {
  $schema: "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
  version: "4.2",
  settings: { qnaEnabled: true },
});

await write(path.join(modelDir, ".platform"), {
  $schema: "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  metadata: { type: "SemanticModel", displayName: projectName },
  config: { version: "2.0", logicalId: guid("bnpl-semantic-model") },
});

await write(path.join(modelDir, "definition", "database.tmdl"), `database ${id("bnpl-database", 32)}
\tcompatibilityLevel: 1702
\tcompatibilityMode: powerBI
\tlanguage: 1033
`);

await write(path.join(modelDir, "definition", "model.tmdl"), `model Model
\tculture: en-US
\tdefaultPowerBIDataSourceVersion: powerBI_V3
\tsourceQueryCulture: en-US
\tdiscourageImplicitMeasures

expression Server = "localhost:5432" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]
expression Database = "bnpl_dw" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]

ref table Transactions
ref table Predictions
ref table 'Model Metrics'
ref table 'Data Quality'
ref table 'Batch Registry'
`);

const transactionTmdl = `table Transactions

\t/// Number of BNPL transactions in the current filter context.
\tmeasure 'Total Transactions' = COUNTROWS(Transactions)
\t\tformatString: #,##0

\t/// Total financed principal in Nigerian naira.
\tmeasure 'Total BNPL Amount' = SUM(Transactions[principal_ngn])
\t\tformatString: N #,##0.00

\t/// Average financed principal in Nigerian naira.
\tmeasure 'Average Loan' = AVERAGE(Transactions[principal_ngn])
\t\tformatString: N #,##0.00

\t/// Share of transactions defaulting within 30 days.
\tmeasure 'Default 30D Rate' = DIVIDE(CALCULATE(COUNTROWS(Transactions), Transactions[default_30d] = TRUE()), [Total Transactions])
\t\tformatString: 0.00%

\t/// Share of transactions defaulting within 90 days.
\tmeasure 'Default 90D Rate' = DIVIDE(CALCULATE(COUNTROWS(Transactions), Transactions[default_90d] = TRUE()), [Total Transactions])
\t\tformatString: 0.00%

\t/// Number of customers in the current filter context.
\tmeasure 'Total Customers' = DISTINCTCOUNT(Transactions[customer_id])
\t\tformatString: #,##0

\t/// Number of 30-day defaults.
\tmeasure '30D Defaults' = CALCULATE(COUNTROWS(Transactions), Transactions[default_30d] = TRUE())
\t\tformatString: #,##0

\t/// Number of 90-day defaults.
\tmeasure '90D Defaults' = CALCULATE(COUNTROWS(Transactions), Transactions[default_90d] = TRUE())
\t\tformatString: #,##0

\tcolumn transaction_id
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: transaction_id

\tcolumn principal_ngn
\t\tdataType: decimal
\t\tsummarizeBy: sum
\t\tsourceColumn: principal_ngn

\tcolumn credit_score
\t\tdataType: int64
\t\tsummarizeBy: average
\t\tsourceColumn: credit_score

\tcolumn default_30d
\t\tdataType: boolean
\t\tsummarizeBy: none
\t\tsourceColumn: default_30d

\tcolumn default_90d
\t\tdataType: boolean
\t\tsummarizeBy: none
\t\tsourceColumn: default_90d

\tcolumn full_date
\t\tdataType: dateTime
\t\tformatString: Short Date
\t\tsummarizeBy: none
\t\tsourceColumn: full_date

\tcolumn year
\t\tdataType: int64
\t\tsummarizeBy: none
\t\tsourceColumn: year

\tcolumn merchant_category
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: merchant_category

\tcolumn provider_name
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: provider_name

\tcolumn customer_state
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: customer_state

\tcolumn customer_id
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: customer_id

\tcolumn first_time_customer
\t\tdataType: boolean
\t\tsummarizeBy: none
\t\tsourceColumn: first_time_customer

\tcolumn credit_score_band
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: credit_score_band

\tcolumn loan_size_category
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: loan_size_category

\tpartition Transactions = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = PostgreSQL.Database(Server, Database, [CreateNavigationProperties=false, Query="select transaction_id, principal_ngn, credit_score, default_30d, default_90d, full_date, year, merchant_category, provider_name, customer_state, customer_id, first_time_customer, credit_score_band, loan_size_category from analytics.vw_bnpl_transactions"])
\t\t\tin
\t\t\t\tSource
`;
await write(path.join(modelDir, "definition", "tables", "Transactions.tmdl"), transactionTmdl);

const auxiliaryTables = {
  "Predictions.tmdl": `table Predictions

\tmeasure 'Streaming Events' = SUM(Predictions[transaction_count])
\t\tformatString: #,##0

\tmeasure 'Average Risk Probability' = AVERAGE(Predictions[average_default_probability])
\t\tformatString: 0.00%

\tcolumn prediction_horizon
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: prediction_horizon

\tcolumn risk_level
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: risk_level

\tcolumn predicted_default
\t\tdataType: boolean
\t\tsummarizeBy: none
\t\tsourceColumn: predicted_default

\tcolumn transaction_count
\t\tdataType: int64
\t\tsummarizeBy: sum
\t\tsourceColumn: transaction_count

\tcolumn average_default_probability
\t\tdataType: double
\t\tsummarizeBy: average
\t\tsourceColumn: average_default_probability

\tcolumn latest_prediction_at
\t\tdataType: dateTime
\t\tsummarizeBy: none
\t\tsourceColumn: latest_prediction_at

\tpartition Predictions = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = PostgreSQL.Database(Server, Database, [CreateNavigationProperties=false, Query="select prediction_horizon, risk_level, predicted_default, transaction_count, average_default_probability, latest_prediction_at from ml.vw_prediction_monitoring"])
\t\t\tin
\t\t\t\tSource
`,
  "Model Metrics.tmdl": `table 'Model Metrics'

\tmeasure 'Model Accuracy' = MAX('Model Metrics'[accuracy])
\t\tformatString: 0.00%

\tmeasure 'Model Precision' = MAX('Model Metrics'[precision_score])
\t\tformatString: 0.00%

\tmeasure 'Model Recall' = MAX('Model Metrics'[recall_score])
\t\tformatString: 0.00%

\tmeasure 'Model F1' = MAX('Model Metrics'[f1_score])
\t\tformatString: 0.00%

\tmeasure 'Model ROC AUC' = MAX('Model Metrics'[roc_auc])
\t\tformatString: 0.000

\tcolumn model_name
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: model_name

\tcolumn model_version
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: model_version

\tcolumn prediction_horizon
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: prediction_horizon

\tcolumn accuracy
\t\tdataType: double
\t\tsummarizeBy: average
\t\tsourceColumn: accuracy

\tcolumn precision_score
\t\tdataType: double
\t\tsummarizeBy: average
\t\tsourceColumn: precision_score

\tcolumn recall_score
\t\tdataType: double
\t\tsummarizeBy: average
\t\tsourceColumn: recall_score

\tcolumn f1_score
\t\tdataType: double
\t\tsummarizeBy: average
\t\tsourceColumn: f1_score

\tcolumn roc_auc
\t\tdataType: double
\t\tsummarizeBy: average
\t\tsourceColumn: roc_auc

\tpartition 'Model Metrics' = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = PostgreSQL.Database(Server, Database, [CreateNavigationProperties=false, Query="select model_name, model_version, prediction_horizon, accuracy, precision_score, recall_score, f1_score, roc_auc from ml.model_metrics"])
\t\t\tin
\t\t\t\tSource
`,
  "Data Quality.tmdl": `table 'Data Quality'

\tmeasure 'DQ Metric Value' = SUM('Data Quality'[metric_value])
\t\tformatString: #,##0.00

\tcolumn pipeline_run_id
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: pipeline_run_id

\tcolumn batch_id
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: batch_id

\tcolumn source
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: source

\tcolumn layer
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: layer

\tcolumn metric_name
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: metric_name

\tcolumn metric_value
\t\tdataType: double
\t\tsummarizeBy: sum
\t\tsourceColumn: metric_value

\tcolumn created_at
\t\tdataType: dateTime
\t\tsummarizeBy: none
\t\tsourceColumn: created_at

\tpartition 'Data Quality' = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = PostgreSQL.Database(Server, Database, [CreateNavigationProperties=false, Query="select pipeline_run_id, batch_id, source, layer, metric_name, metric_value, created_at from data_quality.data_quality_metrics"])
\t\t\tin
\t\t\t\tSource
`,
  "Batch Registry.tmdl": `table 'Batch Registry'

\tmeasure 'Batch Rows' = SUM('Batch Registry'[row_count])
\t\tformatString: #,##0

\tcolumn batch_id
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: batch_id

\tcolumn source
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: source

\tcolumn row_count
\t\tdataType: int64
\t\tsummarizeBy: sum
\t\tsourceColumn: row_count

\tcolumn status
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: status

\tcolumn bronze_status
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: bronze_status

\tcolumn silver_status
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: silver_status

\tcolumn gold_status
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: gold_status

\tcolumn processed_at
\t\tdataType: dateTime
\t\tsummarizeBy: none
\t\tsourceColumn: processed_at

\tpartition 'Batch Registry' = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = PostgreSQL.Database(Server, Database, [CreateNavigationProperties=false, Query="select batch_id, source, row_count, status, bronze_status, silver_status, gold_status, processed_at from pipeline.pipeline_batches"])
\t\t\tin
\t\t\t\tSource
`,
};

for (const [file, content] of Object.entries(auxiliaryTables)) {
  await write(path.join(modelDir, "definition", "tables", file), content);
}

await write(path.join(reportDir, "definition.pbir"), {
  $schema: "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
  version: "4.0",
  datasetReference: { byPath: { path: `../${projectName}.SemanticModel` } },
});

await write(path.join(reportDir, ".platform"), {
  $schema: "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  metadata: { type: "Report", displayName: projectName },
  config: { version: "2.0", logicalId: guid("bnpl-report") },
});

await write(path.join(definitionDir, "version.json"), {
  $schema: "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
  version: "2.0.0",
});

await write(path.join(definitionDir, "report.json"), {
  $schema: "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.2.0/schema.json",
  themeCollection: {
    baseTheme: {
      name: "CY24SU10",
      reportVersionAtImport: { visual: "2.6.0", report: "3.0.0", page: "2.3.0" },
      type: "SharedResources",
    },
  },
});

const expr = {
  column: (table, property) => ({ Column: { Expression: { SourceRef: { Entity: table } }, Property: property } }),
  measure: (table, property) => ({ Measure: { Expression: { SourceRef: { Entity: table } }, Property: property } }),
};
const projection = (field, table, property) => ({ field, queryRef: `${table}.${property}`, nativeQueryRef: property });
const titleVco = (text) => ({
  title: [{ properties: {
    show: { expr: { Literal: { Value: "true" } } },
    text: { expr: { Literal: { Value: `'${text}'` } } },
    bold: { expr: { Literal: { Value: "true" } } },
    fontSize: { expr: { Literal: { Value: "12D" } } },
  } }],
  border: [{ properties: {
    show: { expr: { Literal: { Value: "true" } } },
    color: { solid: { color: { expr: { Literal: { Value: "'#D9E2E8'" } } } } },
    radius: { expr: { Literal: { Value: "6D" } } },
    width: { expr: { Literal: { Value: "1D" } } },
  } }],
  background: [{ properties: {
    show: { expr: { Literal: { Value: "true" } } },
    color: { solid: { color: { expr: { Literal: { Value: "'#FFFFFF'" } } } } },
    transparency: { expr: { Literal: { Value: "0D" } } },
  } }],
});
const baseVisual = (key, position, visual) => ({
  $schema: "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.9.0/schema.json",
  name: id(key),
  position,
  visual,
});
const pageTitle = (key, text) => baseVisual(key, { x: 24, y: 12, z: 1000, height: 56, width: 700, tabOrder: 1000 }, {
  visualType: "textbox",
  objects: { general: [{ properties: { paragraphs: [{ textRuns: [{ value: text, textStyle: { fontFamily: "Segoe UI Semibold", fontSize: "24px", color: "#172B3A" } }], horizontalTextAlignment: "left" }] } }] },
  visualContainerObjects: {
    background: [{ properties: { show: { expr: { Literal: { Value: "false" } } } } }],
    border: [{ properties: { show: { expr: { Literal: { Value: "false" } } } } }],
    padding: [{ properties: {
      top: { expr: { Literal: { Value: "0D" } } },
      bottom: { expr: { Literal: { Value: "0D" } } },
      left: { expr: { Literal: { Value: "0D" } } },
      right: { expr: { Literal: { Value: "0D" } } },
    } }],
  },
});
const card = (key, measures, position) => baseVisual(key, position, {
  visualType: "cardVisual",
  query: { queryState: { Data: { projections: measures.map(([table, measure]) => projection(expr.measure(table, measure), table, measure)) } } },
  objects: { outline: [{ properties: { show: { expr: { Literal: { Value: "false" } } } }, selector: { id: "default" } }] },
  visualContainerObjects: titleVco("Key performance indicators"),
});
const chart = (key, type, category, values, position, title, series) => {
  const roles = {
    Category: { projections: [projection(expr.column(category[0], category[1]), category[0], category[1])] },
    Y: { projections: values.map(([table, measure]) => projection(expr.measure(table, measure), table, measure)) },
  };
  if (series) roles.Series = { projections: [projection(expr.column(series[0], series[1]), series[0], series[1])] };
  return baseVisual(key, position, { visualType: type, query: { queryState: roles }, visualContainerObjects: titleVco(title) });
};
const treemap = (key, group, values, position, title) => baseVisual(key, position, {
  visualType: "treemap",
  query: { queryState: {
    Group: { projections: [projection(expr.column(group[0], group[1]), group[0], group[1])] },
    Values: { projections: values.map(([table, measure]) => projection(expr.measure(table, measure), table, measure)) },
  } },
  visualContainerObjects: titleVco(title),
});
const matrix = (key, rows, values, position, title) => baseVisual(key, position, {
  visualType: "pivotTable",
  query: { queryState: {
    Rows: { projections: rows.map(([table, column]) => projection(expr.column(table, column), table, column)) },
    Values: { projections: values.map(([table, measure]) => projection(expr.measure(table, measure), table, measure)) },
  } },
  objects: { columnHeaders: [{ properties: {
    autoSizeColumnWidth: { expr: { Literal: { Value: "true" } } },
    columnAdjustment: { expr: { Literal: { Value: "'growToFit'" } } },
  } }] },
  visualContainerObjects: { ...titleVco(title), stylePreset: [{ properties: { name: { expr: { Literal: { Value: "'None'" } } } } }] },
});
const slicer = (key, table, column, position, title, mode = "Dropdown") => baseVisual(key, position, {
  visualType: "slicer",
  query: { queryState: { Values: { projections: [projection(expr.column(table, column), table, column)] } } },
  objects: {
    data: [{ properties: { mode: { expr: { Literal: { Value: `'${mode}'` } } } } }],
    header: [{ properties: {
      show: { expr: { Literal: { Value: "true" } } },
      text: { expr: { Literal: { Value: `'${title}'` } } },
    } }],
  },
  visualContainerObjects: {
    ...titleVco(title),
    title: [{ properties: { show: { expr: { Literal: { Value: "false" } } } } }],
  },
});

const pages = [
  {
    name: `ReportSection${id("overview", 24)}`,
    displayName: "Overview",
    visuals: [
      pageTitle("overview-title", "BNPL Portfolio Overview"),
      slicer("overview-date", "Transactions", "full_date", { x: 760, y: 8, z: 2000, height: 80, width: 240, tabOrder: 2000 }, "Date", "Between"),
      slicer("overview-provider", "Transactions", "provider_name", { x: 1016, y: 8, z: 3000, height: 80, width: 240, tabOrder: 3000 }, "Provider"),
      card("overview-kpis", [["Transactions", "Total Transactions"], ["Transactions", "Total BNPL Amount"], ["Transactions", "Average Loan"], ["Transactions", "Default 30D Rate"], ["Transactions", "Default 90D Rate"]], { x: 24, y: 88, z: 4000, height: 120, width: 1232, tabOrder: 4000 }),
      chart("overview-trend", "lineChart", ["Transactions", "full_date"], [["Transactions", "Total BNPL Amount"]], { x: 24, y: 224, z: 5000, height: 464, width: 760, tabOrder: 5000 }, "Financed amount over time"),
      chart("overview-provider-chart", "clusteredBarChart", ["Transactions", "provider_name"], [["Transactions", "Total BNPL Amount"]], { x: 800, y: 224, z: 6000, height: 464, width: 456, tabOrder: 6000 }, "Amount by provider"),
    ],
  },
  {
    name: `ReportSection${id("risk", 24)}`,
    displayName: "Risk Analysis",
    visuals: [
      pageTitle("risk-title", "Default Risk Analysis"),
      slicer("risk-provider", "Transactions", "provider_name", { x: 760, y: 8, z: 2000, height: 80, width: 240, tabOrder: 2000 }, "Provider"),
      slicer("risk-category", "Transactions", "merchant_category", { x: 1016, y: 8, z: 3000, height: 80, width: 240, tabOrder: 3000 }, "Merchant category"),
      chart("risk-credit", "clusteredColumnChart", ["Transactions", "credit_score_band"], [["Transactions", "Default 30D Rate"], ["Transactions", "Default 90D Rate"]], { x: 24, y: 96, z: 4000, height: 280, width: 600, tabOrder: 4000 }, "Default rate by credit score band"),
      chart("risk-loan", "clusteredColumnChart", ["Transactions", "loan_size_category"], [["Transactions", "Default 30D Rate"], ["Transactions", "Default 90D Rate"]], { x: 640, y: 96, z: 5000, height: 280, width: 616, tabOrder: 5000 }, "Default rate by loan size"),
      chart("risk-provider-chart", "clusteredBarChart", ["Transactions", "provider_name"], [["Transactions", "30D Defaults"], ["Transactions", "90D Defaults"]], { x: 24, y: 392, z: 6000, height: 296, width: 600, tabOrder: 6000 }, "Defaults by provider"),
      treemap("risk-merchant", ["Transactions", "merchant_category"], [["Transactions", "90D Defaults"]], { x: 640, y: 392, z: 7000, height: 296, width: 616, tabOrder: 7000 }, "90D defaults by merchant category"),
    ],
  },
  {
    name: `ReportSection${id("customer", 24)}`,
    displayName: "Customer and Geography",
    visuals: [
      pageTitle("customer-title", "Customer and Geography"),
      slicer("customer-state", "Transactions", "customer_state", { x: 760, y: 8, z: 2000, height: 80, width: 240, tabOrder: 2000 }, "State"),
      slicer("customer-provider", "Transactions", "provider_name", { x: 1016, y: 8, z: 3000, height: 80, width: 240, tabOrder: 3000 }, "Provider"),
      card("customer-kpis", [["Transactions", "Total Customers"], ["Transactions", "Total Transactions"], ["Transactions", "Average Loan"]], { x: 24, y: 88, z: 4000, height: 120, width: 1232, tabOrder: 4000 }),
      chart("customer-state-chart", "clusteredBarChart", ["Transactions", "customer_state"], [["Transactions", "Total Transactions"]], { x: 24, y: 224, z: 5000, height: 464, width: 760, tabOrder: 5000 }, "Transaction volume by state"),
      chart("customer-type", "donutChart", ["Transactions", "first_time_customer"], [["Transactions", "Total Transactions"]], { x: 800, y: 224, z: 6000, height: 224, width: 456, tabOrder: 6000 }, "First-time and returning customers"),
      chart("customer-year", "clusteredColumnChart", ["Transactions", "year"], [["Transactions", "Total Customers"]], { x: 800, y: 464, z: 7000, height: 224, width: 456, tabOrder: 7000 }, "Customers by year"),
    ],
  },
  {
    name: `ReportSection${id("streaming", 24)}`,
    displayName: "Streaming Predictions",
    visuals: [
      pageTitle("streaming-title", "Streaming Risk Predictions"),
      card("streaming-kpis", [["Predictions", "Streaming Events"], ["Predictions", "Average Risk Probability"]], { x: 24, y: 80, z: 2000, height: 120, width: 1232, tabOrder: 2000 }),
      chart("streaming-risk", "donutChart", ["Predictions", "risk_level"], [["Predictions", "Streaming Events"]], { x: 24, y: 216, z: 3000, height: 240, width: 400, tabOrder: 3000 }, "Events by risk level"),
      chart("streaming-horizon", "clusteredColumnChart", ["Predictions", "prediction_horizon"], [["Predictions", "Streaming Events"]], { x: 440, y: 216, z: 4000, height: 240, width: 400, tabOrder: 4000 }, "Events by horizon"),
      chart("streaming-default", "clusteredColumnChart", ["Predictions", "predicted_default"], [["Predictions", "Average Risk Probability"]], { x: 856, y: 216, z: 5000, height: 240, width: 400, tabOrder: 5000 }, "Probability by predicted class"),
      matrix("streaming-table", [["Predictions", "prediction_horizon"], ["Predictions", "risk_level"], ["Predictions", "predicted_default"]], [["Predictions", "Streaming Events"], ["Predictions", "Average Risk Probability"]], { x: 24, y: 472, z: 6000, height: 216, width: 1232, tabOrder: 6000 }, "Prediction monitoring detail"),
    ],
  },
  {
    name: `ReportSection${id("monitoring", 24)}`,
    displayName: "Platform Monitoring",
    visuals: [
      pageTitle("monitor-title", "Platform and Model Monitoring"),
      matrix("monitor-model-table", [["Model Metrics", "prediction_horizon"], ["Model Metrics", "model_name"]], [["Model Metrics", "Model Accuracy"], ["Model Metrics", "Model Precision"], ["Model Metrics", "Model Recall"], ["Model Metrics", "Model F1"], ["Model Metrics", "Model ROC AUC"]], { x: 24, y: 80, z: 2000, height: 280, width: 760, tabOrder: 2000 }, "Model comparison"),
      chart("monitor-recall", "clusteredColumnChart", ["Model Metrics", "model_name"], [["Model Metrics", "Model Recall"]], { x: 800, y: 80, z: 3000, height: 280, width: 456, tabOrder: 3000 }, "Recall by model", ["Model Metrics", "prediction_horizon"]),
      matrix("monitor-dq", [["Data Quality", "batch_id"], ["Data Quality", "metric_name"]], [["Data Quality", "DQ Metric Value"]], { x: 24, y: 376, z: 4000, height: 312, width: 600, tabOrder: 4000 }, "Data quality metrics"),
      matrix("monitor-batch", [["Batch Registry", "batch_id"], ["Batch Registry", "source"], ["Batch Registry", "status"], ["Batch Registry", "bronze_status"], ["Batch Registry", "silver_status"], ["Batch Registry", "gold_status"]], [["Batch Registry", "Batch Rows"]], { x: 640, y: 376, z: 5000, height: 312, width: 616, tabOrder: 5000 }, "Pipeline batch registry"),
    ],
  },
];

await write(path.join(definitionDir, "pages", "pages.json"), {
  $schema: "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
  pageOrder: pages.map((page) => page.name),
  activePageName: pages[0].name,
});

for (const page of pages) {
  const pageDir = path.join(definitionDir, "pages", page.name);
  await write(path.join(pageDir, "page.json"), {
    $schema: "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json",
    name: page.name,
    displayName: page.displayName,
    displayOption: "FitToPage",
    height: 720,
    width: 1280,
    objects: {
      background: [{ properties: {
        color: { solid: { color: { expr: { Literal: { Value: "'#F4F7F8'" } } } } },
        transparency: { expr: { Literal: { Value: "0D" } } },
      } }],
    },
  });
  for (const visual of page.visuals) {
    await write(path.join(pageDir, "visuals", visual.name, "visual.json"), visual);
  }
}

console.log(`Generated ${pages.length} Power BI pages in ${root}`);
