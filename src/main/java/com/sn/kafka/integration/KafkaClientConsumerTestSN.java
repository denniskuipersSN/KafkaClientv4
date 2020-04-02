package com.sn.kafka.integration;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.log4j.PropertyConfigurator;
import scala.util.parsing.combinator.testing.Str;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

import static java.util.Collections.singleton;


public class KafkaClientConsumerTestSN {

    private static Integer duration = 10;

    public KafkaClientConsumerTestSN() {

    }

    private static Consumer<String, String> createNConsumer(Properties props) {

        String topic = props.getProperty("topicN");
        String ConsumerConfigFile = props.getProperty("ConsumerNConfgFile");
        if (props.containsKey ("javax.net.ssl.trustStore")) {
            System.setProperty("javax.net.ssl.trustStore",props.getProperty("javax.net.ssl.trustStore"));
            System.setProperty("javax.net.ssl.trustStorePassword",props.getProperty("javax.net.ssl.trustStorePassword"));
            System.setProperty("javax.net.ssl.keyStore",props.getProperty("javax.net.ssl.keyStore"));
            System.setProperty("javax.net.ssl.keyStorePassword",props.getProperty("javax.net.ssl.keyStorePassword"));
        }

        Properties propsConsumer = new Properties();
        try (InputStream input = new FileInputStream(ConsumerConfigFile)) {

            propsConsumer.load(input);
            //if (propsConsumer.containsKey("ssl.truststore.location")) {
            //    propsConsumer.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, props.getProperty("ssl.truststore.location"));
            //    propsConsumer.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, props.getProperty("ssl.truststore.password"));
            //    propsConsumer.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, props.getProperty("ssl.keystore.location"));
            //    propsConsumer.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, props.getProperty("ssl.key.password"));
            //}
            propsConsumer.put("bootstrap.servers",propsConsumer.get("bootstrap.servers"));
            propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,propsConsumer.getProperty("key.deserializer"));
            propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, propsConsumer.getProperty("value.deserializer"));
            propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, propsConsumer.getProperty("group.id"));

            for (Object key: propsConsumer.keySet()) {
                System.out.println(key + ": " + propsConsumer.getProperty(key.toString()));
            }

        }catch (IOException io) {
            io.printStackTrace();
        }
        //String groupid = props.getProperty("group1");
        // Create the consumer using props.
        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(propsConsumer);
        //Consumer<String,String> consumer = new KafkaConsumer<String, String>(props);

        // Subscribe to the topic.
        consumer.subscribe(Arrays.asList(topic));
        //consumer.subscribe(Collections.singletonList(topic));
        //System.out.println(consumer.listTopics());
        System.out.println("Connected and connected to topic : " + topic);
        return consumer;
    }

    private static Consumer<String, GenericRecord> createConsumer(Properties props) {

        String topic = props.getProperty("topic");
        duration = Integer.parseInt (props.getProperty ("duration"));
        String SchemaRegistryURL = props.getProperty("schema_url");
        String ConsumerConfigFile = props.getProperty("ConsumerConfgFile");
        if (props.containsKey ("javax.net.ssl.trustStore")) {
          System.setProperty("javax.net.ssl.trustStore",props.getProperty("javax.net.ssl.trustStore"));
          System.setProperty("javax.net.ssl.trustStorePassword",props.getProperty("javax.net.ssl.trustStorePassword"));
          System.setProperty("javax.net.ssl.keyStore",props.getProperty("javax.net.ssl.keyStore"));
          System.setProperty("javax.net.ssl.keyStorePassword",props.getProperty("javax.net.ssl.keyStorePassword"));
        }

        Properties propsConsumer = new Properties();
        try (InputStream input = new FileInputStream(ConsumerConfigFile)) {

            propsConsumer.load(input);
            //if (propsConsumer.containsKey("ssl.truststore.location")) {
            //    propsConsumer.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, props.getProperty("ssl.truststore.location"));
            //    propsConsumer.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, props.getProperty("ssl.truststore.password"));
            //    propsConsumer.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, props.getProperty("ssl.keystore.location"));
            //    propsConsumer.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, props.getProperty("ssl.key.password"));
            //}
            propsConsumer.put("bootstrap.servers",propsConsumer.get("bootstrap.servers"));
            propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,propsConsumer.getProperty("key.deserializer"));
            propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, propsConsumer.getProperty("value.deserializer"));
            propsConsumer.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SchemaRegistryURL);
            propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, propsConsumer.getProperty("group.id"));

            for (Object key: propsConsumer.keySet()) {
                System.out.println(key + ": " + propsConsumer.getProperty(key.toString()));
            }

        }catch (IOException io) {
            io.printStackTrace();
        }
        //String groupid = props.getProperty("group1");
        // Create the consumer using props.
        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(propsConsumer);
        //Consumer<String,String> consumer = new KafkaConsumer<String, String>(props);

        // Subscribe to the topic.
        consumer.subscribe(Arrays.asList(topic));
        //consumer.subscribe(Collections.singletonList(topic));
        //System.out.println(consumer.listTopics());
        System.out.println("Connected and connected to topic : " + topic);
        return consumer;
    }

    private long getCount(KafkaConsumer consumer, String topic) {
        try {
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            List<PartitionInfo> partitionInfos = topics.get(topic);
            if (partitionInfos == null) {
                return 0;
            } else {
                Collection<TopicPartition> partitions = new ArrayList<>();
                for (PartitionInfo partitionInfo : partitionInfos) {
                    TopicPartition partition = new TopicPartition(topic, partitionInfo.partition());
                    partitions.add(partition);
                }
                Map<TopicPartition, Long> endingOffsets = consumer.endOffsets(partitions);
                Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
                return diffOffsets(beginningOffsets, endingOffsets);
            }
        } catch (Exception e)
        {

        }
        return 0;
    }

    private long diffOffsets(Map<TopicPartition, Long> beginning, Map<TopicPartition, Long> ending) {
        long retval = 0;
        for (TopicPartition partition : beginning.keySet()) {
            Long beginningOffset = beginning.get(partition);
            Long endingOffset = ending.get(partition);
            System.out.println("Begin = " + beginningOffset + ", end = " + endingOffset + " for partition " + partition);
            if (beginningOffset != null && endingOffset != null) {
                retval += (endingOffset - beginningOffset);
            }
        }
        return retval;
    }

    private static String runConsumer(Properties prop) throws InterruptedException {
        StringBuilder stringBuilder = new StringBuilder(100);
        stringBuilder.append ("[");
        try (Consumer<String, GenericRecord> consumer = createConsumer(prop)) {
            // add kafka producer stats, which are rates
            Duration duration1 = Duration.ofSeconds(duration);
            try {
                    ConsumerRecords<String, GenericRecord> records = consumer.poll(duration1);
                    for (ConsumerRecord<String, GenericRecord> record : records) {
                         stringBuilder.append("{\"record\" : {\"topic\" : \""+ record.topic () + "\", \"partition\" : " +  record.partition () + ",\"offset\" : " + record.offset () + ",\"data\" : " );
                        //System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                        //System.out.println (record.offset () + " "  + record.partition () + " " + record.headers ());
                        stringBuilder.append(record.value() + "}}");
                        if( records.iterator ().hasNext ())
                        {
                            stringBuilder.append ("," + "\n");
                        }

                    }
                    stringBuilder.replace (stringBuilder.length ()-2,stringBuilder.length ()-1,"");
                    System.out.println("Reading done :" + records.count());
            } catch (Exception e){
                e.printStackTrace();
            }
            finally {
                consumer.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        stringBuilder.append ("]");
        return stringBuilder.toString();
    }

    public static void getStats(String[] args){
        System.setSecurityManager(null);
        Properties prop = getConfigFile(args);
        StringBuilder sb = new StringBuilder();
        sb.append ("[");
        System.out.println ("test stats");
        Date date = new Date();
        try (Consumer<String, GenericRecord> consumer = createConsumer(prop)) {
            Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics = consumer.metrics();
            Duration duration = Duration.ofSeconds(0);
            consumer.poll (duration);
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            List<PartitionInfo> partitionInfos = topics.get(prop.getProperty ("topic"));
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition inputPartition = new TopicPartition(prop.getProperty ("topic"),partitionInfo.partition () );
                long endoffset = 0;
                long beginoffset = 0;
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(singleton(inputPartition));
                Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets (singleton(inputPartition));
                if (endOffsets.containsKey(inputPartition)) {
                    endoffset = endOffsets.get(inputPartition);
                }
                if (beginOffsets.containsKey(inputPartition)) {
                    beginoffset = beginOffsets.get(inputPartition);
                }
                sb.append ("{\"record\" : {\"data\" : {");
                sb.append ("\"name\" : \"endoffset\"," );
                sb.append ("\"timestamp\" : \"").append (date.getTime ()).append ("\",");
                sb.append ("\"partition\" : \"").append (partitionInfo.partition ()).append ("\",");
                sb.append ("\"description\" : \"").append ("endoffset").append ("\",");
                sb.append ("\"topic\" : \"").append (partitionInfo.topic ()).append ("\",");
                sb.append ("\"value\" : \"").append (endoffset);
                sb.append ("\"}}}");
                sb.append ("," + "\n");

                sb.append ("{\"record\" : {\"data\" : {");
                sb.append ("\"name\" : \"beginOffsets\"," );
                sb.append ("\"timestamp\" : \"").append (date.getTime ()).append ("\",");
                sb.append ("\"partition\" : \"").append (partitionInfo.partition ()).append ("\",");
                sb.append ("\"description\" : \"").append ("beginOffsets").append ("\",");
                sb.append ("\"topic\" : \"").append (partitionInfo.topic ()).append ("\",");
                sb.append ("\"value\" : \"").append (beginoffset );
                sb.append ("\"}}}");
                sb.append ("," + "\n");
            }
            for( Map.Entry<MetricName, ? extends Metric> me : metrics.entrySet () ) {
                sb.append("{\"record\" : {\"data\" : {" );
                //System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                //System.out.println (record.offset () + " "  + record.partition () + " " + record.headers ());
                sb.append ("\"name\" : \"").append (me.getKey ().name ()).append ("\",");
                sb.append ("\"timestamp\" : \"").append (date.getTime ()).append ("\",");
                sb.append ("\"group\" : \"").append (me.getKey ().group ()).append ("\",");
                sb.append ("\"description\" : \"").append (me.getKey ().description ()).append ("\",");
                sb.append ("\"tags\" : \"").append (me.getKey ().tags ()).append ("\",");
                sb.append ("\"value\" : \"").append (me.getValue ().metricValue ().toString ());
                sb.append("\"}}}");
                sb.append ("," + "\n");
            }
        }
        sb.replace (sb.length ()-2,sb.length ()-1,"");
        sb.append ("]");
        System.out.println (sb.toString ());
    }

    private static String runNConsumer(Properties prop) throws InterruptedException {
        StringBuilder stringBuilder = new StringBuilder(100);
        try (Consumer<String, String> consumer = createNConsumer(prop)) {
            Duration duration = Duration.ofSeconds(10);
            try {
                ConsumerRecords<String, String> records = consumer.poll(duration);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                    stringBuilder.append(","+record.value());
                }
                System.out.println("Reading done :" + records.count());
            } catch (Exception e){
                e.printStackTrace();
            }
            finally {
                consumer.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return stringBuilder.toString();
    }

    private static Properties getConfigFile(String[] args){
        CliArgs cliArgs = new CliArgs(args);
        String configfile   = cliArgs.switchValue("-configfile");
        System.out.println("ConfigFile : " + configfile);
        Properties prop = new Properties();
        try (InputStream input = new FileInputStream(configfile)) {
            prop.load(input);
            String log4jConfigFile = prop.getProperty("log4jfile");
            System.out.println("log4j file : " + log4jConfigFile);
            PropertyConfigurator.configure(log4jConfigFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

    public static String runKafkaClient(String[] args) throws Exception {

        Properties prop = getConfigFile(args);
        System.setSecurityManager(null);
        System.out.println("Start Consumer 1111");
        return runConsumer(prop);
    }

    public static String mainCaller(String[] args) throws Exception {
        System.setSecurityManager(null);
        String[] configfile = {"-configfile","src/main/java/resources/testSNKafka.properties"};
        if (args.length == 1)
            args = configfile;
        Properties prop = getConfigFile(args);
        String Messages = runNConsumer(prop);
        System.out.println(Messages);
        return Messages;
    }

    public static void main(String[] args) throws Exception {
        System.setSecurityManager(null);
        String Messages = "";
        String[] newArgs = {args[1],args[2]};
        //System.out.println(args[0] + " "  + args[1] + " " + args[2] + " " + args.length);
        String[] args1 = {"-configfile","src/main/java/resources/testSNKafka.properties"};
        if (args.length == 3) {
            if (args[0].contains ("runKafkaClient")) {
                System.out.println ("Executing runKafkaClient");
                Messages = runKafkaClient (newArgs);
                System.exit (0);
            }
            if (args[0].contains ("getStats")) {
                System.out.println ("Executing getStats");
                getStats (newArgs);
                System.exit (0);
            }
        }
        if (args.length == 0)
           args = args1;
        Properties prop = getConfigFile(args);
        Messages = runConsumer(prop);
        System.out.println(Messages);
    }

}