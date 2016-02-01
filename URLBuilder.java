import java.io.*;
import java.io.BufferedReader;
import java.lang.Boolean;
import java.lang.String;
import java.lang.System;
import java.lang.reflect.Array;
import java.util.*;
import java.util.ArrayList;



class URLBuilder
{

    ArrayList<String> keywordsList =new ArrayList<String>();
    String ipFile,keywordsFile;
    Map<String, ArrayList<String>> ipList = new HashMap<String, ArrayList<String>>();

    URLBuilder(String _ipFile, String _keywordsFile) {

        ipFile = _ipFile;
        keywordsFile = _keywordsFile;

        BufferedReader br = null;

        IPKeyword(keywordsFile);    //read keywords file

        ArrayList<String> ipset = null;


        //Reading IPList file and extracting ip's w.r.t different interval slot
        //ipList => hashmap, contains keys as per interval along with corresponding ip's

        try {
            String sCurrentLine;
            br = new BufferedReader(new FileReader(ipFile));
            while ((sCurrentLine = br.readLine()) != null) {
                Boolean matched = sCurrentLine.matches("[0-9]*_min");

                if (matched == true) {

                    String minutes[] = new String[2];
                    minutes = sCurrentLine.split("_");
                    //System.out.println(minutes[0]);
                    ipset = new ArrayList<String>();


                    ipList.put(minutes[0], ipset);
                } else {
                    ipset.add(sCurrentLine);
                }

            }

        } catch (IOException e) {
            //e.printStackTrace();
        }

    }
    ArrayList<String> BuildURL(long currentMinute)
    {

        ArrayList<String> DM_ip=new ArrayList<String>();

        //System.out.println("Current Minute: "+currentMinute);

        int a[] = {1, 3, 10, 30, 59};
        for (int i = 0; i < a.length; i++) {

            if (currentMinute%a[i] ==0) {
                ArrayList<String> ip_keyword=addKeyword(ipList.get(Integer.toString(a[i])));
                DM_ip.addAll(ip_keyword);
            }
        }
        //System.out.println("No of calls to be made: "+DM_ip.size() +"\n"+DM_ip);
        System.out.println("No of calls to be made: "+DM_ip.size());


        return DM_ip;

    }





    void IPKeyword(String keywords)
    {

        BufferedReader br=null;
        try{

            String word;
            br=new BufferedReader(new FileReader(keywords));
            while((word=br.readLine())!=null)
            {
                keywordsList.add(word);
            }

        }catch(IOException e){
            //e.printStackTrace();
        }


    }
    public ArrayList<String> addKeyword(ArrayList<String> ipList)
    {

        Iterator itr=ipList.iterator();

        ArrayList<String>ip_keyword=new ArrayList<String>();
        String ip_key=null;
        //------Sends the URL to the Kafka Producer-----//
        while(itr.hasNext())
        {
            ip_key=itr.next()+","+getKeyword();
            ip_keyword.add(ip_key);
        }
        return ip_keyword;

    }
    public String getKeyword()
    {

        String randomKeyword = keywordsList.get(new Random().nextInt(keywordsList.size()));
        return randomKeyword;
    }



}
