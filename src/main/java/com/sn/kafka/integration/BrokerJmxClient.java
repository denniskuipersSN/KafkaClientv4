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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
        Set<ObjectInstance> getAllBeans = mbsc.queryMBeans (null,null);
        Iterator<ObjectInstance> iterator = getAllBeans.iterator();
        while (iterator.hasNext()) {
            ObjectInstance instance = iterator.next ();
            System.out.println ("Class Name: " + instance.getClassName ());
            System.out.println ("Object Name: " + instance.getObjectName ());
            System.out.println (mbsc.getMBeanInfo (instance.getObjectName ()).toString ());
         }
        return mbsc;
    }

    public JmxReporter.MeterMBean createSocketMbean() throws Exception
    {

        ObjectName mbeanName = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec");
        JmxReporter.MeterMBean stats  = JMX.newMBeanProxy(getMbeanConnection(), mbeanName, JmxReporter.MeterMBean.class, true);
        return stats;
    }

    public String getBrokerStats() throws Exception
    {
        StringBuffer buf = new StringBuffer();
        JmxReporter.MeterMBean stats = createSocketMbean();
        buf.append("count : " + stats.getCount () + " mean : " + stats.getMeanRate () + " minute " + stats.getOneMinuteRate ()) ;
        return buf.toString();
    }



    public static void main(String[] args) throws Exception {
        BrokerJmxClient JMXNew =  new BrokerJmxClient("172.31.31.73",9111,1);
        System.out.println (JMXNew.getBrokerStats ());
        //System.out.println (getMBeanName("class", "kafka:type=kafka.SocketServerStats"));
    }
}