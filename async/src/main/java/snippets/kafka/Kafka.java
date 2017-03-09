package snippets.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sun.istack.internal.NotNull;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created by rnair on 3/9/17.
 */
public class Kafka {

    private static String serverAddress;
    private static String topicName;
    private static long timeOut;

    private static final Logger logger = Logger.getLogger(Kafka.class);


    static {
        BasicConfigurator.configure();
    }

    @BeforeClass
    public static void setUp() throws IOException{
        logger.info("Reading config file");
        ConfigReader configReader = new ObjectMapper(new YAMLFactory()).readValue(new File("config/kafka-config.yml"), ConfigReader.class);

        serverAddress = configReader.serverAddress;
        topicName = configReader.topicName;
        timeOut = configReader.timeOut;
    }


    @Test
    public void testProduceToTopic() throws JsonProcessingException {

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
            sendItemToTopic("item1", mapper.writeValueAsString(testkafKfkaItem), topicName, producer, timeOut);

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
                    } catch (InterruptedException|ExecutionException | TimeoutException e) {
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
            logger.error(e,e);
            return null;
        });
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
    public String topicName = "kafka-test";

    @NotNull
    public long timeOut = 5000;
}
