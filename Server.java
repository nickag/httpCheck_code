import java.net.URL;
import java.net.HttpURLConnection;
import java.io.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Date;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.collection.Seq;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchResponse;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;

import java.nio.ByteBuffer;
import java.util.*;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import java.io.UnsupportedEncodingException;


class SKafkaConsumer extends  Thread {
    final static String clientId = "SimpleConsumerDemoClient";
    final static String TOPIC = "response";
    ConsumerConnector consumerConnector;



    public SKafkaConsumer(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect","localhost:2181");
        properties.put("group.id","test-group");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        String msg=null;
        while(it.hasNext())
        {
            try {


                Server.fw = new FileWriter("httpLog.html", true);
                try {
                    //Server.fw =new FileWriter("httpLog.html",true);
                    msg = new String(it.next().message());
                    System.out.println(msg);
                    String[] msgList;
                    msgList = msg.split(";");
                    String log = "<tr>";
                    URL url = new URL(msgList[0]);
                    msgList[0] = url.getHost();
                    for (int i = 0; i < msgList.length; i++) {
                        log += "<td>" + msgList[i] + "</td>";
                    }
                    log += "</tr>";
                    Server.fw.write(log);
                    //Server.fw.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    Server.fw.close();
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

        }


    }

    private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        for(MessageAndOffset messageAndOffset: messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(new String(bytes, "UTF-8"));
        }
    }
}

class SKafkaProducer {
    final static String TOPIC = "request";



    public static void send(String msg){

        Properties properties = new Properties();
        properties.put("metadata.broker.list","localhost:9092");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
        KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC,msg);
        producer.send(message);
        //SimpleDateFormat sdf = new SimpleDateFormat();
        producer.close();
    }
    public void sendMsg(String msg)
    {


        KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC,msg);
        //producer.send(message);
    }

}





public class Server{
    public static FileWriter fw;
    static String logHTTP=null;
    public static void main(String[] argv) throws Exception
    {


        System.out.println(argv[0]);
        String fileName=argv[0];

        fw =new FileWriter("httpLog.html");
        logHTTP="<html><head><title>HTTP Check</title><meta charset='utf-8'>  <meta name='viewport' content='width=device-width, initial-scale=1'>  <link rel='stylesheet' href='http://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css'>  <script src='https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js'></script>  <script src='http://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js'></script></head><body><div class='page-header'><center><h1>HTTP Check</h1></center></div><table class='table table-striped'><tr><th>URL</th><th>Status Code</th><th>DNS Lookup Time(sec)</th><th>TTFB(sec)</th><<th>Total_Time(sec)</th>th>Response Size(bytes)</th></tr>";
        fw.write(logHTTP);
        fw.close();

        SKafkaConsumer consumer=new SKafkaConsumer();
        consumer.start();

        while(true)
        {


            //-----------Reads URL from file----------------//

            BufferedReader br=null;

            br=new BufferedReader(new InputStreamReader(System.in));
            //System.out.println("Enter a url file: ");

            //String fileName=br.readLine();

            ArrayList<String> urlist=new ArrayList<String>();
            try{
                String sCurrentLine;
                br=new BufferedReader(new FileReader(fileName));
                while((sCurrentLine=br.readLine())!=null)
                {
                    urlist.add(sCurrentLine);
                }

            }catch(IOException e){
                //e.printStackTrace();
            }


            Iterator itr=urlist.iterator();

            long start=new Date().getTime();

            //------Sends the URL to the Kafka Producer-----//
            while(itr.hasNext())
            {
                String url=(String)itr.next();
                //System.out.println(url);
                SKafkaProducer.send(url);
            }

            //----------waits for the next iteration / Webpage Test-------------//
            try {
                Thread.sleep(120000);                 //1000 milliseconds is one second.
            } catch(InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
		    /*try{
		        consumer.join();
		    }
		    catch(Exception e)
		    {
		        e.printStackTrace();
		    }*/

        }

        //fw.write(logHTTP);
        //fw.close();
        //float timeElapsed=(new Date().getTime()-start)/1000f;
        //System.out.println(response.i+ " Time Elapsed: "+timeElapsed+" seconds");

    }




}
