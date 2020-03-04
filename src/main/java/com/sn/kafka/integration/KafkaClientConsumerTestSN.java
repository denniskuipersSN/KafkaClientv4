package com.sn.kafka.integration;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class KafkaClientConsumerTestSN {

    public KafkaClientConsumerTestSN() {

    }

    private static Consumer<String, GenericRecord> createConsumer(Properties props) {

        String topic = props.getProperty("topic");
        String SchemaRegistryURL = props.getProperty("schema_url");
        String ConsumerConfigFile = props.getProperty("ConsumerConfgFile");

        Properties propsConsumer = new Properties();
        try (InputStream input = new FileInputStream(ConsumerConfigFile)) {

            propsConsumer.load(input);
            if (props.containsKey("ssl.truststore.location")) {
                propsConsumer.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, props.getProperty("ssl.truststore.location"));
                propsConsumer.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, props.getProperty("ssl.truststore.password"));
                propsConsumer.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, props.getProperty("ssl.keystore.location"));
                propsConsumer.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, props.getProperty("ssl.key.password"));
            }
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

    private static String runConsumer(Properties prop) throws InterruptedException {
        StringBuilder stringBuilder = new StringBuilder(100);
        String s = "";
        try (Consumer<String, GenericRecord> consumer = createConsumer(prop)) {
            Duration duration = Duration.ofSeconds(10);
            try {
                    ConsumerRecords<String, GenericRecord> records = consumer.poll(duration);
                    for (ConsumerRecord<String, GenericRecord> record : records) {
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
        System.out.println("Start Consumer");
        return runConsumer(prop);
    }

    public static int mainCaller() throws Exception {
        String[] args = {"-configfile","/var/lib/jenkins/workspace/KafkaPipeLine@2/src/main/java/resources/testSNKafka.properties"};
        main(args);
        return 10;
    }

    public static void main(String[] args) throws Exception {

        String[] args1 = {"-configfile","/var/lib/jenkins/workspace/KafkaPipeLine@2/src/main/java/resources/testSNKafka.properties"};
        if (args.length == 0)
             args = args1;
        Properties prop = getConfigFile(args);
        String Messages = runConsumer(prop);
        System.out.println(Messages);
    }

}