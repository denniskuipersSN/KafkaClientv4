import org.junit.jupiter.api.Test;
import com.sn.kafka.integration.KafkaAvroProducer;
import com.sn.kafka.integration.KafkaClientConsumerTestSN;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class myTests {

    @Test
    public void TestProducer() {
        KafkaAvroProducer tester = new KafkaAvroProducer(); // MyClass is tested
        String[] args = {"-configfile","/var/lib/jenkins/workspace/KafkaIntegration/src/main/java/resources/testSNKafka.properties"};

        // assert statements
        assertEquals(0, tester);
    }
}