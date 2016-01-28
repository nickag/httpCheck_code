
import java.net.URL;
import java.net.HttpURLConnection;
import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Date;
import java.net.SocketTimeoutException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.collection.Seq;
import java.net.*;
import java.text.SimpleDateFormat;
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
import java.text.DecimalFormat;
class Test {

    public native double[] sayHello(String url);

    /*static {
        System.loadLibrary("clib");
    }*/
}

class CKafkaConsumer extends  Thread {
    final static String clientId = "SimpleConsumerDemoClient";
    final static String TOPIC = "request";
    ConsumerConnector consumerConnector;



    public CKafkaConsumer(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect","localhost:2181");
        String consumerGroup=Client.consumerGroup;
        properties.put("group.id",consumerGroup);
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
            msg=new String(it.next().message());
            System.out.println(msg);
            Client.urlList.add(msg);
            response r=new response(msg);
            Client.executor.execute(r);
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

class CKafkaProducer {

    static String TOPIC = "response";
    static kafka.javaapi.producer.Producer<String,String> producer =null;

    public static void create(String topic){
        TOPIC=topic;
        Properties properties = new Properties();
        properties.put("metadata.broker.list","localhost:9092");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        //kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
        producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
        //KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC,msg);
        //producer.send(message);
        //SimpleDateFormat sdf = new SimpleDateFormat();
        //producer.close();
    }
    public static void sendMsg(String msg)
    {
        KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC,msg);
        producer.send(message);
    }

    public static void close()
    {
        producer.close();
    }

}



class response implements Runnable{
    static int i=0;
    String USER_AGENT;
    String Url="http://www.google.co.in";
    public response(String url)
    {
        this.Url=url;
        USER_AGENT= "Mozilla/5.0 ;Windows NT 6.2; WOW64; rv:27.0; Gecko/20100101 Firefox/27.0";
    }


    public void run()
    {
        String msg=Url;

        int responseCode=500;
        float dnsLookupTime=0,elapsedTime=0,ttfb=0;
        int responseSize=0;

	String Host="";
	try{
		URL url=new URL(Url);
		Host=url.getHost();	
	}
	catch(Exception e)
	{
		e.printStackTrace();
	}
	msg+=";"+Host;
        System.loadLibrary("Test");
        double[] results=new Test().sayHello(Url);
	msg+=";"+Integer.toString((int)results[0]);
	DecimalFormat f=new DecimalFormat("00.00");
	
        for(int i=1;i<results.length;i++)
        {
            String val=Double.toString(results[i]);
            System.out.println(val);
            msg+=";"+val;

        }
        System.out.println(msg);
        CKafkaProducer.sendMsg(msg);
        /*try{

            URL obj=new URL(Url);
            String host=obj.getHost();
            long start=new Date().getTime();
            InetAddress ip=InetAddress.getByName(host);
            dnsLookupTime=(new Date().getTime()-start)/1000f;

            HttpURLConnection con= (HttpURLConnection)obj.openConnection();
            //System.out.println((new Date().getTime()-start)/1000f);
            con.setRequestMethod("GET");
            con.setRequestProperty("User-Agent",USER_AGENT);
            //System.out.println(con.getConnectTimeout());
            //con.setConnectTimeout(2000);
            con.setReadTimeout(5000);
            start=new Date().getTime();
            //con.connect();
            responseCode=con.getResponseCode();
            //elapsedTime=(new Date().getTime()-start)/1000f;

            if (responseCode<HttpURLConnection.HTTP_BAD_REQUEST){
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();
                int firstByte=0;
                while ((inputLine = in.readLine()) != null) {
                    if (firstByte==0)
                    {
                        firstByte=1;
                        ttfb=(new Date().getTime()-start)/1000f;
                    }

                    response.append(inputLine);
                }
                byte responseBytes[]=response.toString().getBytes();
                responseSize=responseBytes.length;
                elapsedTime=(new Date().getTime()-start)/1000f;	//total_time
                in.close();

                //System.out.println(response.toString());
            } else
                System.out.println("GET request Failed(400/500) Error : "+Url);

            System.out.println(Url+" ; Dns Lookup Time: "+dnsLookupTime+"ms ; Time Taken: "+elapsedTime+" ms ;  Response Code: "+responseCode+ " ; Response Size: "+responseSize+" ; "+Thread.currentThread().getName());
        }
        catch(UnknownHostException ue){

            System.out.println(ue);
        }
        catch(SocketTimeoutException te){
            responseCode=408;
            System.out.println(te);
        }
        catch(Exception e){
            e.printStackTrace();

        }
        finally{

            msg+=Integer.toString(responseCode)+";"+ Float.toString(dnsLookupTime)+";"+Float.toString(ttfb)+";"+Float.toString(elapsedTime)+";"+Integer.toString(responseSize);
            System.out.println(msg);
            CKafkaProducer.sendMsg(msg);
        }*/

    }

}



public class Client{
    static ArrayList<String> urlList=new ArrayList<String>();

    static ExecutorService executor=null;
    static String consumerGroup;
    public static void main(String argv[]) throws Exception
    {
        consumerGroup=argv[0];
        System.out.println(consumerGroup);
        /*
        BufferedReader br=null;
        br=new BufferedReader(new InputStreamReader(System.in));
        String fileName=br.readLine();
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

        int len=urlist.size();
        int threadPool=len/2;
        //Iterator itr=urlist.iterator();

        /*while(itr.hasNext())
        {
            response r= new response();
            String url=(String)itr.next();
            r.setUrl(url);
            Thread t=new Thread(r);
            t.start();
            //System.out.println(itr.next());
        }
    */

        CKafkaProducer.create("response");
        executor=Executors.newFixedThreadPool(50);
        CKafkaConsumer urlReq=new CKafkaConsumer();
        urlReq.start();
        Iterator itr=urlList.iterator();


        /*
        long start=new Date().getTime();
        //ExecutorService executor=Executors.newFixedThreadPool(threadPool);
        while(itr.hasNext())
        {
            String url=(String)itr.next();
            System.out.println(url);
            //response r=new response(url);
            //executor.execute(r);
        }

        //executor.shutdown();
        //while(!executor.isTerminated()){}
        //KafkaProducer.close();
        float timeElapsed=(new Date().getTime()-start)/1000f;
        System.out.println(response.i+ " Time Elapsed: "+timeElapsed+" seconds");

        */

    }




}
