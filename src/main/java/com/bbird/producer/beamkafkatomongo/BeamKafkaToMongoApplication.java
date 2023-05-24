package com.bbird.producer.beamkafkatomongo;

import com.bbird.producer.beamkafkatomongo.options.IKafkaPipelineOptions;
import kong.unirest.json.JSONObject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;
import org.joda.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@SpringBootApplication
public class BeamKafkaToMongoApplication {

	static String bootstrapServer = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092";

	static String saslmechansim = "PLAIN";

	static String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"26HLC5BMXMI2XYQH\" password=\"lye9ES6kaDcNC6Zl0M4UX8uUmh7NMTm8nk1gfdpHcr9dkasIgBTXM+hylKZ+9tZ3\";";
	static String securityProtocol = "SASL_SSL";

	public static PCollection<KafkaRecord<String,String>> readFromKafka(Pipeline pipeline,IKafkaPipelineOptions options) {
		Map<String,Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
		props.put(SaslConfigs.SASL_MECHANISM,saslmechansim);
		props.put(SaslConfigs.SASL_JAAS_CONFIG,jaasConfig);
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,securityProtocol);
		props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

		props.put(ConsumerConfig.GROUP_ID_CONFIG,"blackbird-consumer-app");
		// now we connect to the queue and process every event
		PCollection<KafkaRecord<String,String>> data =  pipeline.apply(
				"ReadFromKafka",
				KafkaIO.<String, String> read()
						.withBootstrapServers(options.getKafkaServer())
						.withConsumerConfigUpdates(props)
						.withTopics(
								Collections.singletonList(options
										.getInputTopic()))
						.withKeyDeserializer(StringDeserializer.class)
						.withValueDeserializer(StringDeserializer.class)
						.commitOffsetsInFinalize());

		return  data;

	}

	public static PCollection<Map<String,String>> processElementsFromKafka(PCollection<KafkaRecord<String,String>> data){
		PCollection<Map<String,String>> kafkaRecordsProcessed = data.apply(ParDo.of(new DoFn<KafkaRecord<String,String>,Map<String,String>>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				String topic = "";
				String payload = "";
				String publishTimeUTC = "";
				String key = null;
				Integer partition  = null;
				Long offset = null;

				KafkaRecord<String,String> records = c.element();
				topic = records.getTopic();
				payload = records.getKV().getValue();
				partition = records.getPartition();
				offset = records.getOffset();
				key = records.getKV().getKey();

				Map<String,String> headerKeys = Arrays.asList(records.getHeaders().toArray())
						.stream()
						.collect(Collectors.toMap(Header::key, e->new String(e.value(), StandardCharsets.UTF_8)
								,(existing,replacement)-> replacement));

				JSONObject mainNode = new JSONObject(payload);

				Map<String,String> headerObj = new HashMap<>();
				if(headerKeys.size() != 0){
					headerKeys.entrySet().forEach(entry -> {
						headerObj.put(entry.getKey(),entry.getValue());
					});
					mainNode.put("KAFAK_HEADER",headerObj);

				}

				JSONObject kafkaInfo = new JSONObject();
				kafkaInfo.put("Partition",partition);
				kafkaInfo.put("Offset",offset);
				kafkaInfo.put("key",key);
				kafkaInfo.put("Topic",topic);

				mainNode.put("Kafka",kafkaInfo);
				payload = mainNode.toString();

				System.out.println("payload" + payload);

				Map<String,String> myData = new HashMap<>();
				myData.put(key,payload);

				c.output(myData);

			}
		}));
		return  kafkaRecordsProcessed;
	}

	public static void dumpRecordsToMongo(IKafkaPipelineOptions options,PCollection<KV<String, String>> recordsWithKV,String collectionName) throws Exception {
		PCollection<Document> recordsToDump = recordsWithKV
				.apply("Convert to BSON Object",ParDo.of(new DoFn<KV<String,String>, Document>() {
					@ProcessElement
					public void processElement(ProcessContext c)  {
						Document row = new Document();
						KV<String, String> element = c.element();
						String values = element.getValue();
						JSONObject jsonObject = new JSONObject(values);
						Iterator<String> keys =  jsonObject.keys();
						String errorMessage = "";
						while (keys.hasNext()) {
							String key = keys.next();
							Object object = jsonObject.get(key);
							String value = jsonObject.get(key).toString();
							if (key.equalsIgnoreCase("contact")) {
								if (value.length() < 10) {
									errorMessage = errorMessage + "Invalid Contact: " + value + ",";
								}
							}
							if (object.getClass().getName().contains("JSONObject")) {
								row.put(key, Document.parse(value));
							} else {
								row.put(key, value);
							}
						}
						row.put("errorMessage",errorMessage);
						c.output(row);
					}
				}));

		recordsToDump.apply("WriteToMongo",
				MongoDbIO.write()
						.withUri(options.getMongoUri())
						.withDatabase(options.getDatabase())
						.withCollection(collectionName)
		);
	}


	public static void main(String[] args) {

		Map<String,Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
		props.put(SaslConfigs.SASL_MECHANISM,saslmechansim);
		props.put(SaslConfigs.SASL_JAAS_CONFIG,jaasConfig);
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,securityProtocol);
		props.put(ProducerConfig.ACKS_CONFIG,"all");
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30 * 1000);
		props.put(ProducerConfig.RETRIES_CONFIG, 5);
		props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);


//		SpringApplication.run(BeamKafkaToMongoApplication.class, args);
		IKafkaPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(IKafkaPipelineOptions.class);

		Pipeline pipeline = Pipeline.create(options);
		PCollection<KafkaRecord<String,String>> data = readFromKafka(pipeline,options);

		PCollection<Map<String,String>> processedRecords = processElementsFromKafka(data);

		PCollection<KV<String, String>> recordsWithKV = processedRecords
				.apply("ExtractPayload",
						ParDo.of(new DoFn<Map<String,String>, KV<String, String>>() {
							@ProcessElement
							public void processElement(ProcessContext c)
									throws Exception {
								Map<String,String> myData = c.element();
								myData.forEach((k,v) -> {
									c.output(KV.of(k, v));
								});
							}
						}));

		try {
			dumpRecordsToMongo(options, recordsWithKV,options.getCollection());
		}catch (Exception e){
			System.out.println(e.getMessage());
			recordsWithKV
					.apply("WriteToKafkaDLQ",
							KafkaIO.<String, String> write()
									.withBootstrapServers(
											options.getKafkaServer())
									.withProducerConfigUpdates(props)
									.withTopic(options.getOutputTopic())
									.withKeySerializer(
											org.apache.kafka.common.serialization.StringSerializer.class)
									.withValueSerializer(
											org.apache.kafka.common.serialization.StringSerializer.class));

		}

		PipelineResult run = pipeline.run();
		run.waitUntilFinish(Duration.standardSeconds(options.getDuration()));

	}

}
