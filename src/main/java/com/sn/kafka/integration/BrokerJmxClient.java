package com.sn.kafka.integration;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import kafka.network.*;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.Sanitizer;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

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

                System.out.println (Type + " " + Name + " count : " + stats.getCount () + " mean : " + stats.getMeanRate () + " minute " + stats.getOneMinuteRate () + " FiveMine Rate : " + stats.getFiveMinuteRate () + " FifteenMinuteRate :  " + stats.getFifteenMinuteRate () + " Unit : "  + stats.getRateUnit ());
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