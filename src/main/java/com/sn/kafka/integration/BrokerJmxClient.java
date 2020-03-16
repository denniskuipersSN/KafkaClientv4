package com.sn.kafka.integration;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import kafka.network.*;
import org.apache.avro.data.Json;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.Sanitizer;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import com.google.gson.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.Timestamp;
import java.util.*;
import com.sn.kafka.integration.KafkaAvroProducer;

import com.yammer.metrics.reporting.*;

public class BrokerJmxClient
{
    private final String host;
    private final int port;
    private final long time;
    public BrokerJmxClient(String host, int port, long time)
    {
        this.host = host;
        this.port = port;
        this.time = time;
    }

    public MBeanServerConnection getMbeanConnection() throws Exception
    {

        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://"+ host+ ":" + port + "/jmxrmi");
        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
        MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
        return mbsc;
    }

    public void createSocketMbean() throws Exception
    {

        MBeanServerConnection JMXServer = getMbeanConnection();
        Set<ObjectInstance> getAllBeans = JMXServer.queryMBeans (null,null);
        Iterator<ObjectInstance> iterator = getAllBeans.iterator();
        JmxReporter.MeterMBean stats = null;
        String Name = "";
        String Type = "";
        Date date = new Date();

        JsonObject MeterMetric = new JsonObject();

        String[] args = {"-configfile","src/main/java/resources/testJMXToKafkaTopic.properties"};
        Properties prop = KafkaAvroProducer.getConfigFile(args);
        KafkaProducer PClient = KafkaAvroProducer.ProducerClient(prop);

        while (iterator.hasNext()) {
            ObjectInstance instance = iterator.next ();
            if (instance.getClassName ().contains("Meter") ) {
                stats = JMX.newMBeanProxy (JMXServer, instance.getObjectName (), JmxReporter.MeterMBean.class, true);
                Hashtable attributes = instance.getObjectName ().getKeyPropertyList ();
                for (Object key : attributes.keySet ()) {
                    if (key.toString ().contains ("name")) {
                        Name = attributes.get (key.toString ()).toString ();
                    } else if(key.toString ().contains ("type"))
                        Type = attributes.get (key.toString ()).toString ();
                }
                MeterMetric.addProperty ("timestamp",date.getTime());
                MeterMetric.addProperty ("type",Type);
                MeterMetric.addProperty ("type",Name);
                MeterMetric.addProperty ("count",stats.getCount ());
                MeterMetric.addProperty ("mean",stats.getMeanRate ());
                MeterMetric.addProperty ("OneMinuteRate",stats.getOneMinuteRate ());
                MeterMetric.addProperty ("FiveMinuteRate",stats.getFiveMinuteRate ());
                MeterMetric.addProperty ("FifteenMinuteRate",stats.getFifteenMinuteRate ());
                MeterMetric.addProperty ("Unit", String.valueOf (stats.getRateUnit ()));
                KafkaAvroProducer.SendJsonMessage (PClient,MeterMetric);

                System.out.println ("Send ; " + MeterMetric);
            }
            //Hashtable attributes = instance.getObjectName ().getKeyPropertyList ();
            //System.out.println (instance.getObjectName ().getKeyPropertyListString ());
            //for (Object key : attributes.keySet ()) {
            //    System.out.println (key.toString ());
            //    if (key.toString ().contains ("Count")) {
            //        System.out.println (JMXServer.getAttribute (instance.getObjectName (), key.toString ()));
            //    }

            //}
        }

        //ObjectName mbeanName = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec");
        //JmxReporter.MeterMBean stats  = JMX.newMBeanProxy(JMXServer, mbeanName, JmxReporter.MeterMBean.class, true);
    }


    public static void main(String[] args) throws Exception {
        BrokerJmxClient JMXNew =  new BrokerJmxClient("172.31.31.73",9111,1);
        JMXNew.createSocketMbean ();
        //System.out.println (JMXNew.getBrokerStats ());
        //System.out.println (getMBeanName("class", "kafka:type=kafka.SocketServerStats"));
    }
}