package snippets.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sun.istack.internal.NotNull;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.*;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import kafka.consumer.Consumer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by rnair on 3/9/17.
 */
public class Kafka {

    private static final Logger logger = Logger.getLogger(Kafka.class);
    private static String serverAddress;
    private static String schemaRegistyAddress;
    private static String topicName;
    private static String avroTopicName;
    private static long timeOut;
    private static String zookeeperAddress;

    static {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
    }

    @BeforeClass
    public static void setUp() throws IOException {
        logger.info("Reading config file");
        ConfigReader configReader = new ObjectMapper(new YAMLFactory()).readValue(new File("config/kafka-config.yml"), ConfigReader.class);

        serverAddress = configReader.serverAddress;
        schemaRegistyAddress = configReader.schemaRegistryAddress;
        topicName = configReader.topicName;
        avroTopicName = configReader.avroTopicName;
        timeOut = configReader.timeOut;
        zookeeperAddress = configReader.zookeeperAddress;
    }


    public void runProduceToTopic() throws JsonProcessingException {

        Properties props = new Properties();
        props.put("bootstrap.servers", serverAddress);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("timeout.ms", 5000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<String, String>(props)) {
            ObjectMapper mapper = new ObjectMapper();
            TestKfkaItem testkafKfkaItem = new TestKfkaItem("test1", new Book("james", "Jan 20, 2017"));
            TestKfkaItem testkafKfkaItem2 = new TestKfkaItem("test2", new Book("fdsfs", "Jan 20, 2017"));
            sendItemToTopic("item1", mapper.writeValueAsString(testkafKfkaItem), topicName, producer, timeOut);
            sendItemToTopic("item2", mapper.writeValueAsString(testkafKfkaItem2), topicName, producer, timeOut);
        }

    }


    private void sendItemToTopic(String key, String data, String topicName, Producer<String, String> producer, long timeOutInMilliSeconds) {

        Future<RecordMetadata> sendFuture = producer.send(new ProducerRecord<String, String>(topicName, key, data));
        CompletableFuture<RecordMetadata> promise = CompletableFuture.supplyAsync(() ->
                {
                    logger.debug("Sending data to " + topicName
                            + "\n" + data);
                    try {
                        return sendFuture.get(timeOutInMilliSeconds, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }
        ).thenApply((v) -> {
            logger.debug("Data successfully sent to topic " + topicName
                    + "\n" + data);
            return v;
        }).exceptionally((e) -> {
            logger.error("Data failed to send to topic " + topicName
                    + "\n" + data);
            logger.error(e, e);
            return null;
        });
    }

    private void sendItemToTopic(String key, GenericRecord data, String topicName, Producer<Object, Object> producer, long timeOutInMilliSeconds) {
        Future<RecordMetadata> sendFuture = producer.send(new ProducerRecord<Object,Object>(topicName,key,data));
        CompletableFuture<RecordMetadata> promise = CompletableFuture.supplyAsync(() ->
                {
                    logger.debug("Sending data to " + topicName
                            + "\n" + data);
                    try {
                        return sendFuture.get(timeOutInMilliSeconds, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }
        ).thenApply((v) -> {
            logger.debug("Data successfully sent to topic " + topicName
                    + "\n" + data);
            return v;
        }).exceptionally((e) -> {
            logger.error("Data failed to send to topic " + topicName
                    + "\n" + data);
            logger.error(e, e);
            return null;
        });
    }


    public void runConsumeFromTopic(String groupName, String consumerName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", serverAddress);
        props.put("group.id", groupName);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "100");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        getItemsFromTopic(consumer,consumerName);
    }


//    public void runConsumeFromTopicWithAvro(String groupName, String consumerName) {
//        Properties props = new Properties();
//        props.put("bootstrap.servers", serverAddress);
//        props.put("group.id", groupName);
//        props.put("enable.auto.commit", "true");
//        props.put("auto.commit.interval.ms", "100");
//        props.put("session.timeout.ms", "30000");
//
//        props.put("zookeeper.connect", zookeeperAddress);
//
//
////        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroDecoder.class);
////        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDecoder.class);
//        props.put("schema.registry.url", schemaRegistyAddress);
//
////        VerifiableProperties vProps = new VerifiableProperties(props);
////        KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
////        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);
//
////        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
////        consumer.subscribe(Arrays.asList(avroTopicName));
////        getItemsFromTopic(consumer,consumerName);
//
//
//
////        Consumer<String,String> consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
////
////        Map<String, List<KafkaStream>> consumerMap = consumer.createMessageStreams(
////                topicCountMap, keyDecoder, valueDecoder);
////        KafkaStream stream = consumerMap.get(topic).get(0);
////        ConsumerIterator it = stream.iterator();
////        while (it.hasNext()) {
////            MessageAndMetadata messageAndMetadata = it.next();
////            try {
////                String key = (String) messageAndMetadata.key();
////                String value = (IndexedRecord) messageAndMetadata.message();
////
////    ...
////            } catch(SerializationException e) {
////                // may need to do something with it
////            }
////        }
//
////
//        Map<String, Integer> topicCountMap = new HashMap<>();
//        topicCountMap.put(avroTopicName, 1);
////
////        ConsumerIterator<String, Object> iterator = Consumer.createJavaConsumerConnector(new ConsumerConfig(props))
////                .createMessageStreams(topicMap, new KafkaAvroDecoder(new VerifiableProperties(props)), new KafkaAvroDecoder(new VerifiableProperties(props)))
////                .get(avroTopicName).get(0).iterator();
////
////        MessageAndMetadata<String, Object> messageAndMetadata = iterator.next();
////        GenericRecord logLine = (GenericRecord) messageAndMetadata.message();
////        logger.info("Here fsdfafasdfgasgadkjglajdflgjakdjgpajiogj'adjfg;ajdg;iaodgjldfgodjoigjdios");
////        logger.info(logLine.get("f1"));
//
//
//
//        VerifiableProperties vProps = new VerifiableProperties(props);
//        KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
//        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);
//
//        ConsumerConnector consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
//
//        Map<String, List<KafkaStream>> consumerMap = kafka.javaapi.consumer.ConsumerConnector.createMessageStreams(
//                topicCountMap, keyDecoder, valueDecoder);
//        KafkaStream stream = consumerMap.get(topicName).get(0);
//        ConsumerIterator it = stream.iterator();
//        while (it.hasNext()) {
//            MessageAndMetadata messageAndMetadata = it.next();
//            try {
//                String key = (String) messageAndMetadata.key();
//                IndexedRecord value = (IndexedRecord) messageAndMetadata.message();
//
//         } catch(SerializationException e) {
//                // may need to do something with it
//                e.printStackTrace();
//            }
//        }
//
//    }


    public void getItemsFromTopic(org.apache.kafka.clients.consumer.Consumer<String, String> consumer, String consumerName) {
        new Thread(() -> {
            Thread.currentThread().setName(consumerName);
            logger.info("Starting consumer to recieve data from " + topicName
                    + "\n");
            try {
                while (true) {
                    logger.info("Polling");
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(String.format("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()));
                    }
                }
            } catch (Exception e) {
                // failed to start
                logger.error(e, e);
            }
        }).start();
    }


    public void runProduceToTopicWithAvro() throws JsonProcessingException {

        Properties props = new Properties();
        props.put("bootstrap.servers", serverAddress);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("timeout.ms", 5000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", schemaRegistyAddress);
        String testItemSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(testItemSchema);


        try (Producer<Object, Object> producer = new KafkaProducer<Object, Object>(props)) {
//            TestKfkaItem testkafKfkaItem = new TestKfkaItem("test1", new Book("james", "Jan 20, 2017"));
//            TestKfkaItem testkafKfkaItem2 = new TestKfkaItem("test2", new Book("fdsfs", "Jan 20, 2017"));
//            sendItemToTopic("item1", mapper.writeValueAsString(testkafKfkaItem), topicName, producer, timeOut);
//            sendItemToTopic("item2", mapper.writeValueAsString(testkafKfkaItem2), topicName, producer, timeOut);
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("f1", "value1");
            sendItemToTopic("item1", avroRecord,avroTopicName,producer,timeOut);

            avroRecord = new GenericData.Record(schema);
            avroRecord.put("f1", "value2");
            sendItemToTopic("item2", avroRecord,avroTopicName,producer,timeOut);
        }

    }


    public void runProduceToTopicWithAvroToVman(String machineName) throws JsonProcessingException {

        Properties props = new Properties();
        props.put("bootstrap.servers", serverAddress);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("timeout.ms", 5000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", schemaRegistyAddress);
       String testItemSchema = "{\"type\":\"record\",\"name\":\"OvaDeploymentEvent\",\"fields\":[{\"name\":\"type\",\"type\":{\"type\":\"enum\",\"name\":\"type\",\"symbols\":[\"OVA\"]},\"default\":\"OVA\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"tenant\",\"type\":\"string\"},{\"name\":\"resourcePath\",\"type\":\"string\"},{\"name\":\"networkLabel\",\"type\":\"string\"},{\"name\":\"folderPath\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(testItemSchema);


        try (Producer<Object, Object> producer = new KafkaProducer<Object, Object>(props)) {
//            TestKfkaItem testkafKfkaItem = new TestKfkaItem("test1", new Book("james", "Jan 20, 2017"));
//            TestKfkaItem testkafKfkaItem2 = new TestKfkaItem("test2", new Book("fdsfs", "Jan 20, 2017"));
//            sendItemToTopic("item1", mapper.writeValueAsString(testkafKfkaItem), topicName, producer, timeOut);
//            sendItemToTopic("item2", mapper.writeValueAsString(testkafKfkaItem2), topicName, producer, timeOut);
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("type", "OVA");
            avroRecord.put("name", machineName);
            avroRecord.put("tenant", "ateam");

            avroRecord.put("resourcePath", "http://10.244.4.2/bdds.ova");

            avroRecord.put("networkLabel", "ateam|ateam-anp|ateam-net005");

            avroRecord.put("folderPath", "Testing2");

            sendItemToTopic("key1", avroRecord,avroTopicName,producer,timeOut);
        }

    }


    public void runGetFromAvroTopic(String groupId, String threadName){


        new Thread(() ->{
            Thread.currentThread().setName(threadName);

            Properties props = new Properties();
            props.put("zookeeper.connect", zookeeperAddress);
            props.put("bootstrap.servers", serverAddress);
            props.put("group.id", groupId);
            props.put("schema.registry.url", schemaRegistyAddress);
            props.put("specific.avro.reader", true);

            // We configure the consumer to avoid committing offsets and to always start consuming from beginning of topic
            // This is not a best practice, but we want the example consumer to show results when running it again and again
            props.put("auto.commit.enable", "false");
            props.put("auto.offset.reset", "smallest");
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

            // Hard coding single threaded consumer
            topicCountMap.put(avroTopicName, 1);

            VerifiableProperties vProps = new VerifiableProperties(props);

            // Create decoders for key and value
            KafkaAvroDecoder avroDecoder = new KafkaAvroDecoder(vProps);
            StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());

            ConsumerConnector consumer = null;

            consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                    new ConsumerConfig(props));

            KafkaStream stream = consumer.createMessageStreams(topicCountMap, stringDecoder, avroDecoder).get(avroTopicName).get(0);
            ConsumerIterator it = stream.iterator();
//            logger.info("Ready to start iterating wih properties: " + props.toString());
//            logger.info("Reading topic:" + avroTopicName);

            while (it.hasNext()) {
                MessageAndMetadata messageAndMetadata = it.next();
//                String key = (String) messageAndMetadata.key();


                // Once we release a new version of the avro deserializer that can return SpecificData, the deep copy will be unnecessary
                GenericRecord genericEvent = (GenericRecord) messageAndMetadata.message();
//                LogLine event = (LogLine) SpecificData.get().deepCopy(LogLine.SCHEMA$, genericEvent);

//                SessionState oldState = state.get(ip);
//                int sessionId = 0;
//                if (oldState == null) {
//                    state.put(ip, new SessionState(event.getTimestamp(), 0));
//                } else {
//                    sessionId = oldState.getSessionId();
//
//                    // if the old timestamp is more than 30 minutes older than new one, we have a new session
//                    if (oldState.getLastConnection() < event.getTimestamp() - sessionLengthMs)
//                        sessionId = sessionId + 1;
//
//                    SessionState newState = new SessionState(event.getTimestamp(), sessionId);
//                    state.put(ip, newState);
//                }
//                event.setSessionid(sessionId);
//                System.out.println(event.toString());
//                ProducerRecord<String, LogLine> record = new ProducerRecord<String, LogLine>(outputTopic, event.getIp().toString(), event);
//                producer.send(record);

                logger.info("pulled");
                String key =  messageAndMetadata.key()==null?null:messageAndMetadata.key().toString().trim();
                String message =messageAndMetadata.message()==null?null:messageAndMetadata.message().toString().trim();
                logger.info("Got item key " + key);
                logger.info("Got Message " + message);
            }


        }).start();


    }


    public static void main (String args[]) throws IOException{
        setUp();
//        new Kafka().runProduceToTopic();
//        new Kafka().runConsumeFromTopic("test", "test-consumer1");
//        new Kafka().runConsumeFromTopic("test2", "test2-consumer1");

        // with avro

//        new Thread( () -> {
//            try {
//                while(true) {
//                    new Kafka().runProduceToTopicWithAvro();
//                    try {
//                        Thread.sleep(2000);
//                    }catch (InterruptedException ex){}
//                }
//            }catch (Exception e){
//                logger.error(e,e);
//            }
//        }).start();
//        new Kafka().runConsumeFromTopicWithAvro("test3", "test3-consumer1");
//
//         new Kafka().runProduceToTopicWithAvroToVman("mac1");

        new Kafka().runGetFromAvroTopic("test1", "test1-consumer1");
    }

}


class TestKfkaItem {

    @JsonProperty("name")
    private String name;

    @JsonProperty("book")
    private Book book;

    @JsonCreator
    public TestKfkaItem(@JsonProperty("name") String name, @JsonProperty("book") Book book) {
        this.name = name;
        this.book = book;
    }

}


class Book {

    @JsonProperty("author")
    private String author;

    @JsonProperty("date")
    private String date;

    @JsonCreator
    public Book(@JsonProperty("author") String author, @JsonProperty("date") String date) {
        this.author = author;
        this.date = date;
    }

}


class ConfigReader {

    @NotNull
    public String serverAddress;

    @NotNull
    public String schemaRegistryAddress;

    @NotNull
    public String topicName = "kafka-test";

    @NotNull
    public String avroTopicName = "kafka-avro-test";

    @NotNull
    public long timeOut = 5000;

    @NotNull
    public String zookeeperAddress;
}


class KafkaResponse {
    private String key;
    private String data;

    public KafkaResponse(String key, String data) {
        this.key = key;
        this.data = data;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
