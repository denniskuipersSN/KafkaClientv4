package com.sn.kafka.integration;

import org.junit.jupiter.api.Test;
import com.sn.kafka.integration.KafkaAvroProducer;
import com.sn.kafka.integration.KafkaClientConsumerTestSN;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestKafkaAvroProducer {

    @Test
    public void testKafkaAvroProducer()  throws Exception {
        // assert statements
        assertEquals(10, KafkaAvroProducer.mainCaller ()) ;
    }

    @Test
    public void testKafkaClientConsumerTestSN()  throws Exception {
        // assert statements
        assertEquals(10, KafkaClientConsumerTestSN.mainCaller ()) ;
    }
}