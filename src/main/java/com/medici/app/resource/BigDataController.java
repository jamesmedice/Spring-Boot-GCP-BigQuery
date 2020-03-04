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

import com.google.cloud.bigquery.Job;
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

}
