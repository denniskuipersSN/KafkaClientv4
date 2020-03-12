package com.sn.kafka.integration;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import kafka.network.*;

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

    public SocketServerStatsMBean createSocketMbean() throws Exception
    {

        ObjectName mbeanName = new ObjectName("kafka:type=kafka.SocketServerStats");
        SocketServerStatsMBean stats = JMX.newMBeanProxy(getMbeanConnection(), mbeanName, SocketServerStatsMBean.class, true);
        return stats;
    }

    public String getBrokerStats() throws Exception
    {
        StringBuffer buf = new StringBuffer();
        SocketServerStatsMBean stats = createSocketMbean();
        buf.append(stats.getNumFetchRequests () / (1024 *1024)  + "," +  stats.getBytesReadPerSecond()  / (1024 *1024) );
        return buf.toString();
    }

    public static void main(String[] args) throws Exception {
        BrokerJmxClient JMXNew =  new BrokerJmxClient("ec2-18-191-188-44.us-east-2.compute.amazonaws.com",9111,1);
        System.out.println (JMXNew.getBrokerStats ());
    }

}