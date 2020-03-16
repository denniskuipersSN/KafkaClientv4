package com.sn.kafka.integration;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.log4j.PropertyConfigurator;
import com.google.gson.*;
import com.google.gson.JsonSerializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaAvroProducer {

    public KafkaAvroProducer(){

    }

    public static KafkaProducer ProducerClient(Properties producerProp)
    {

        String topic = producerProp.getProperty("topic");
        String SchemaRegistryURL = "";
        if (producerProp.containsKey ("schema_url"))
           SchemaRegistryURL = producerProp.getProperty ("schema_url");
        String ProducerConfigFile = producerProp.getProperty("ProducerConfgFile");

        Properties props = new Properties();
        Properties propsProducer = new Properties();
        try (InputStream input = new FileInputStream(ProducerConfigFile)) {
            propsProducer.load(input);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, propsProducer.getProperty("bootstrap.servers"));
            if (propsProducer.containsKey("ssl.truststore.location")) {
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, propsProducer.getProperty("ssl.truststore.location"));
                props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, propsProducer.getProperty("ssl.truststore.password"));
                props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, propsProducer.getProperty("ssl.keystore.location"));
                props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, propsProducer.getProperty("ssl.key.password"));
            }
        }catch (Exception e){

        }

        props.put(ProducerConfig.CLIENT_ID_CONFIG, topic);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, propsProducer.getProperty("key.serializer"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,propsProducer.getProperty("value.serializer"));
        if (!SchemaRegistryURL.isEmpty ())
           props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SchemaRegistryURL);
        KafkaProducer producer = new KafkaProducer(props);
        return producer;

    }
    public static void SendAvroMessage(KafkaProducer producer) {
        String key = "key11";
        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser ();
        Schema schema = parser.parse (userSchema);
        GenericRecord avroRecord = new GenericData.Record (schema);
        avroRecord.put ("f1", "value1");
        //System.out.println("Sending Message" + avroRecord.getSchema().toString());
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<> ("testAvro12", key, avroRecord);
        try {
            producer.send (record);
        } catch (Exception e) {
            e.printStackTrace ();
            // may need to do something wth it
        }
        // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
        // then close the producer to free its resources.
        try {
            System.out.println ("Flush");
            producer.flush ();
            System.out.println ("Finished flush");
            producer.close ();
            System.out.println ("Finished close");
        } catch (Exception e) {
            System.out.println (e);
        }
        System.out.println ("Finished");
    }

    public static void SendJsonMessage(KafkaProducer producer, JsonObject Json) {
        //System.out.println("Sending Message" + avroRecord.getSchema().toString());
        ProducerRecord<String, JsonObject> record = new ProducerRecord<> ("MetricTopic", Json);
        try {
            producer.send (record);
        } catch (Exception e) {
            e.printStackTrace ();
            // may need to do something wth it
        }
        // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
        // then close the producer to free its resources.
        try {
            System.out.println ("Flush");
            producer.flush ();
            System.out.println ("Finished flush");
            producer.close ();
            System.out.println ("Finished close");
        } catch (Exception e) {
            System.out.println (e);
        }
        System.out.println ("Finished");
    }
    public static Properties getConfigFile(String[] args){
        String Test = "111hallo11111111";
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

    public static int mainCaller() throws Exception {
        String[] args = {"-configfile","src/main/java/resources/testSNKafka.properties"};
        main(args);
        return 10;
    }

    public static void main(String[] args) throws Exception {
        System.setSecurityManager(null);
        Properties prop = getConfigFile(args);
        KafkaProducer PClient = ProducerClient(prop);
        SendAvroMessage(PClient);
        System.out.println("Done");
    }
}