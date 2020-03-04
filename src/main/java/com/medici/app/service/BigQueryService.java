package com.medici.app.service;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.bigquery.core.BigQueryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.multipart.MultipartFile;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.medici.app.config.AutoConfiguration.BigQueryFileGateway;

@Service
public class BigQueryService {

	@Autowired
	BigQueryFileGateway bigQueryFileGateway;

	@Autowired
	BigQueryTemplate bigQueryTemplate;

	@Value("${spring.cloud.gcp.bigquery.datasetName}")
	private String datasetName;

	public ListenableFuture<Job> writeDataToTable(MultipartFile file, String tableName) throws IOException {
		return this.bigQueryTemplate.writeDataToTable(tableName, file.getInputStream(), FormatOptions.csv());

	}

	public ListenableFuture<Job> insertBigQueryTable(String csvData, String tableName) {
		return this.bigQueryFileGateway.insertBigQueryTable(csvData.getBytes(), tableName);
	}

}
