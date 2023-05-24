package com.bbird.producer.beamkafkatomongo.options;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

import java.text.SimpleDateFormat;
import java.util.Date;

public interface IKafkaPipelineOptions extends PipelineOptions {
    @Description("Kafka Bootstrap Servers")
    @Default.String("localhost:9092")
    String getKafkaServer();

    void setKafkaServer(String value);

    @Description("Kafka Topic Name")
    @Default.String("bbird.customer.topic")
    String getInputTopic();

    void setInputTopic(String value);

    @Description("Kafka DLQ topic")
    @Default.String("bbird.dlq.customer.topic")
    String getOutputTopic();

    void setOutputTopic(String value);

    @Description("Pipeline duration to wait until finish in seconds")
    @Default.Long(-1)
    Long getDuration();

    @Description("MongoDb default uri")
    @Default.String("mongodb://parasmani347:71ihGnuXokKgCV3L@ac-pgoono4-shard-00-00.myokxqe.mongodb.net:27017,ac-pgoono4-shard-00-01.myokxqe.mongodb.net:27017,ac-pgoono4-shard-00-02.myokxqe.mongodb.net:27017/?ssl=true&replicaSet=atlas-10ze0i-shard-0&authSource=admin&retryWrites=true&w=majority")
    String getMongoUri();
    void setMongoUri(String mongoUri);

    @Description("Pipeline duration to wait until finish in seconds")
    @Default.String("myDatabase")
    String getDatabase();
    void setDatabase(String database);

    @Description("Pipeline duration to wait until finish in seconds")
    @Default.String("Customer")
    String getCollection();
    void setCollection(String collection);



    void setDuration(Long duration);

    @Description("Check Error exists")
    @Default.Boolean(false)
    Boolean getHasError();

    void setHasError(Boolean value);


    class GDELTFileFactory implements DefaultValueFactory<String> {
        public String create(PipelineOptions options) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            String date = format.format(new Date());
            return date.toString();
        }
    }

}
