package com.medici.app.service;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.bigquery.core.BigQueryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.multipart.MultipartFile;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQuery.TableDataListOption;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.medici.app.config.AutoConfiguration.BigQueryFileGateway;

@Service
public class BigQueryService {

	@Autowired
	BigQueryFileGateway bigQueryFileGateway;

	@Autowired
	BigQueryTemplate bigQueryTemplate;

	@Autowired
	BigQuery bigquery;

	@Value("${spring.cloud.gcp.bigquery.datasetName}")
	private String datasetName;

	public String getDatasetName() throws IOException {
		return this.bigQueryTemplate.getDatasetName();
	}

	public ListenableFuture<Job> writeDataToTable(MultipartFile file, String tableName) throws IOException {
		return this.bigQueryTemplate.writeDataToTable(tableName, file.getInputStream(), FormatOptions.csv());
	}

	public ListenableFuture<Job> insertBigQueryTable(String csvData, String tableName) {
		return this.bigQueryFileGateway.insertBigQueryTable(csvData.getBytes(), tableName);
	}

	public Page<Dataset> listDatasets(DatasetListOption options) {
		return bigquery.listDatasets(options);
	}

	public Page<Table> listTables(String datasetId, TableListOption options) {
		return bigquery.listTables(datasetId, options);
	}

	public TableResult listTableData(String dataset, String table, long pageSize) {
		TableId tableId = TableId.of(dataset, table);
		return bigquery.listTableData(tableId, TableDataListOption.pageSize(pageSize));
	}

}
