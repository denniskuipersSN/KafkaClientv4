package com.sn.kafka.integration;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

//import com.apple.eawt.AppEvent;
import com.yammer.metrics.reporting.JmxReporter;
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

//import com.yammer.metrics.reporting.AbstractReporter.*;

import static com.yammer.metrics.reporting.JmxReporter.*;
import static java.lang.System.exit;

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

    public void createSocketMbean(String[] configfile ) throws Exception
    {

        MBeanServerConnection JMXServer = getMbeanConnection();
        Set<ObjectInstance> getAllBeans = JMXServer.queryMBeans (null,null);
        Iterator<ObjectInstance> iterator = getAllBeans.iterator();
        JmxReporter.MeterMBean stats = null;
        String Name = "";
        String Type = "";
        Date date = new Date();
        JsonObject MeterMetric = new JsonObject();
        String[] configfileSRC = {"-configfile","src/main/java/resources/testJMXToKafkaTopic.properties"};
        if (configfile.length == 0)
            configfile = configfileSRC;
        Properties prop = KafkaAvroProducer.getConfigFile(configfile);
        KafkaProducer PClient = KafkaAvroProducer.ProducerClient(prop);
        while (true)
        {
          try{
            while (iterator.hasNext()) {
              ObjectInstance instance = iterator.next ();
              if (instance.getClassName ().contains ("Meter")) {
                  stats = JMX.newMBeanProxy (JMXServer, instance.getObjectName (), MeterMBean.class, true);
                  Hashtable attributes = instance.getObjectName ().getKeyPropertyList ();
                  for (Object key : attributes.keySet ()) {
                      if (key.toString ().contains ("name")) {
                          Name = attributes.get (key.toString ()).toString ();
                      } else if (key.toString ().contains ("type"))
                          Type = attributes.get (key.toString ()).toString ();
                  }
                  MeterMetric.addProperty ("timestamp", date.getTime ());
                  MeterMetric.addProperty ("type", Type);
                  MeterMetric.addProperty ("type", Name);
                  MeterMetric.addProperty ("count", stats.getCount ());
                  MeterMetric.addProperty ("mean", stats.getMeanRate ());
                  MeterMetric.addProperty ("OneMinuteRate", stats.getOneMinuteRate ());
                  MeterMetric.addProperty ("FiveMinuteRate", stats.getFiveMinuteRate ());
                  MeterMetric.addProperty ("FifteenMinuteRate", stats.getFifteenMinuteRate ());
                  MeterMetric.addProperty ("Unit", String.valueOf (stats.getRateUnit ()));
                  KafkaAvroProducer.SendJsonMessage (PClient, MeterMetric.toString ());

                  System.out.println ("Send ; " + MeterMetric);
              }
            }
              Thread.sleep (30000000);
           }catch (Exception e)
          {
              System.out.println (e);
              System.out.println ("For Testing purpose 123412");
              exit(1);
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

    public static int RunCollection(String[] configfile) throws Exception
    {
        System.setSecurityManager(null);
        BrokerJmxClient JMXNew =  new BrokerJmxClient("172.31.31.73",9111,1);
        JMXNew.createSocketMbean (configfile);
        return 0;
    }
    public static void main(String[] args) throws Exception {
        System.setSecurityManager(null);
        BrokerJmxClient JMXNew =  new BrokerJmxClient("172.31.31.73",9111,1);
        String[] configfile = {"-configfile","src/main/java/resources/testSNKafka.properties"};
        JMXNew.createSocketMbean (configfile);
        //System.out.println (JMXNew.getBrokerStats ());
        //System.out.println (getMBeanName("class", "kafka:type=kafka.SocketServerStats"));
    }
}