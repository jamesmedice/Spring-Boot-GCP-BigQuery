package com.medici.app.resource;

import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.medici.app.entity.QueryMessage;
import com.medici.app.service.BigQueryService;

/**
 * 
 * @author a73s
 *
 */
@RestController
public class BigDataController {

	protected Logger logger = Logger.getLogger(BigDataController.class.getName());

	@Autowired
	BigQueryService bigQueryService;

	@RequestMapping(value = "/push", method = RequestMethod.POST)
	public ResponseEntity<?> uploadText(@RequestBody QueryMessage message) {

		if (message.getCsvDataRow().isEmpty()) {
			return new ResponseEntity(false, HttpStatus.OK);
		}

		try {
			ListenableFuture<Job> payloadJob = bigQueryService.insertBigQueryTable(message.getCsvDataRow(), message.getTableName());
			return new ResponseEntity(payloadJob, new HttpHeaders(), HttpStatus.OK);

		} catch (Exception e) {
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}

	}

	@RequestMapping(value = "/upload/{tableName}", consumes = { "multipart/form-data" }, method = RequestMethod.POST)
	public ResponseEntity<?> uploadFile(@RequestParam(value = "file", required = true) MultipartFile uploadfile, @PathVariable("tableName") String tableName) {

		if (uploadfile.isEmpty()) {
			return new ResponseEntity(false, HttpStatus.OK);
		}

		try {
			ListenableFuture<Job> payloadJob = bigQueryService.writeDataToTable(uploadfile, tableName);
			return new ResponseEntity(payloadJob, new HttpHeaders(), HttpStatus.OK);

		} catch (Exception e) {
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}

	}

	@RequestMapping(value = "/{dataset}/{table}/{pageSize}", method = RequestMethod.GET)
	public TableResult listTableData(@PathVariable("dataset")  String dataset ,@PathVariable("table")  String table, @PathVariable("pageSize")  long pageSize) {

			TableResult payload = bigQueryService.listTableData(dataset, table, pageSize);
			return payload;

	}

	@RequestMapping(value = "/", method = RequestMethod.GET)
	public ResponseEntity<?> getDatasetName(  ) {

		try {
			String payload = bigQueryService.getDatasetName();
			return new ResponseEntity(payload, new HttpHeaders(), HttpStatus.OK);

		} catch (Exception e) {
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}

	}

	@RequestMapping(value = "/datasets", method = RequestMethod.POST)
	public ResponseEntity<?> listDatasets(@RequestBody DatasetListOption options) {

		try {
			Page<Dataset> payload = bigQueryService.listDatasets(options);
			return new ResponseEntity(payload, new HttpHeaders(), HttpStatus.OK);

		} catch (Exception e) {
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}

	}

	@RequestMapping(value = "/dataset/{datasetId}", method = RequestMethod.POST)
	public ResponseEntity<?> listTables(@PathVariable("datasetId") String datasetId, @RequestBody TableListOption options) {

		try {
			Page<Table> payload = bigQueryService.listTables(datasetId, options);
			return new ResponseEntity(payload, new HttpHeaders(), HttpStatus.OK);

		} catch (Exception e) {
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}

	}

}
