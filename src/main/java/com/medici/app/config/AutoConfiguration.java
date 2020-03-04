package com.medici.app.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.gcp.bigquery.core.BigQueryTemplate;
import org.springframework.cloud.gcp.bigquery.integration.BigQuerySpringMessageHeaders;
import org.springframework.cloud.gcp.bigquery.integration.outbound.BigQueryFileMessageHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.gateway.GatewayProxyFactoryBean;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.util.concurrent.ListenableFuture;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;

@Configuration
@EnableAutoConfiguration
public class AutoConfiguration {

	@Bean
	public DirectChannel bigQueryInsertDataChannel() {
		return new DirectChannel();
	}

	@Bean
	public DirectChannel bigQueryReplyChannel() {
		return new DirectChannel();
	}

	@Bean
	@ServiceActivator(inputChannel = "bigQueryInsertDataChannel")
	public MessageHandler messageSender(BigQueryTemplate bigQueryTemplate) {
		BigQueryFileMessageHandler messageHandler = new BigQueryFileMessageHandler(bigQueryTemplate);
		messageHandler.setFormatOptions(FormatOptions.csv());
		messageHandler.setOutputChannel(bigQueryReplyChannel());
		return messageHandler;
	}

	@Bean
	public GatewayProxyFactoryBean gatewayProxyFactoryBean() {
		GatewayProxyFactoryBean factoryBean = new GatewayProxyFactoryBean(BigQueryFileGateway.class);
		factoryBean.setDefaultRequestChannel(bigQueryInsertDataChannel());
		factoryBean.setDefaultReplyChannel(bigQueryReplyChannel());
		factoryBean.setAsyncExecutor(null);
		return factoryBean;
	}

	@MessagingGateway
	public interface BigQueryFileGateway {
		ListenableFuture<Job> insertBigQueryTable(byte[] csvData, @Header(BigQuerySpringMessageHeaders.TABLE_NAME) String tableName);
	}

}
