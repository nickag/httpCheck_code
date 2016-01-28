#include<stdio.h>
#include<stdlib.h>
#include<curl/curl.h>
#include<jni.h>
#include"Test.h"

static size_t WriteCallback(void *ptr, size_t size, size_t nmemb, void *data)
{
  /* we are not interested in the downloaded bytes itself,
     so we only return the size we would have saved ... */
  (void)ptr;  /* unused */
  (void)data; /* unused */
  return (size_t)(size * nmemb);
}

JNIEXPORT jdoubleArray JNICALL Java_Test_sayHello(JNIEnv *env, jobject obj, jstring URL){

    const char *url = (*env)->GetStringUTFChars(env, URL, NULL);
    printf("\t Hey Java!\n");
    curl_global_init( CURL_GLOBAL_ALL );
    CURL * myHandle;
    jdouble val,dns,conn,preTime,ttfb,ttime,redirectTime,redirectCount,size,downSpeed,resCode;
    val=dns=conn=preTime=ttfb=ttime=redirectTime=redirectCount=size=downSpeed=resCode=0;
    int no=0;
    jdoubleArray result=(*env)->NewDoubleArray(env, 10);
    CURLcode res; // We’ll store the result of CURL’s webpage retrieval, for simple error checking.
    myHandle = curl_easy_init( );
    // Notice the lack of major error checking, for brevity
    curl_easy_setopt(myHandle, CURLOPT_URL, url);
    curl_easy_setopt(myHandle, CURLOPT_USERAGENT,"Mozilla/4.0");
    curl_easy_setopt(myHandle, CURLOPT_TIMEOUT_MS, 2000L);
    curl_easy_setopt(myHandle, CURLOPT_WRITEFUNCTION, WriteCallback);
    res = curl_easy_perform( myHandle );
    if(CURLE_OK == res) {
  
        
        /* check for total download time */

      
          res = curl_easy_getinfo(myHandle, CURLINFO_NAMELOOKUP_TIME, &dns);
        if((CURLE_OK == res) && (dns>0))
        {
            printf("Name lookup time: %0.3f sec.\n", dns);
            
        }


          /* check for connect time */
          res = curl_easy_getinfo(myHandle, CURLINFO_CONNECT_TIME, &conn);
          if((CURLE_OK == res) && (conn>0))
          {

            //result[no++]=val;
            printf("Connect time: %0.3f sec.\n", conn);
        }

          res = curl_easy_getinfo(myHandle, CURLINFO_PRETRANSFER_TIME, &preTime);
          if((CURLE_OK == res) && (preTime>0))
          {

            //result[no++]=val;
            printf("Pretransfer time: %0.3f sec.\n", preTime);
        }


        res = curl_easy_getinfo(myHandle, CURLINFO_STARTTRANSFER_TIME, &ttfb);
          if((CURLE_OK == res) && (ttfb>0)){
            printf("TTFB: %0.3f sec.\n", ttfb);
            //result[no++]=val;
        }

        res = curl_easy_getinfo(myHandle, CURLINFO_TOTAL_TIME, &ttime);
        if((CURLE_OK == res) && (ttime>0)){
          printf("Total Response time: %0.3f sec.\n", ttime);
          //result[no++]=val;
      }

          res = curl_easy_getinfo(myHandle, CURLINFO_REDIRECT_TIME, &redirectTime);
          if((CURLE_OK == res) || (redirectTime>0))
          {

            //result[no++]=val;
            printf("Redirect Time: %0.3f sec.\n", redirectTime);
        }

          res = curl_easy_getinfo(myHandle, CURLINFO_REDIRECT_COUNT, &redirectCount);
          if((CURLE_OK == res) || (redirectCount>0))
          {

            //result[no++]=val;
            printf("Redirect Count: %f\n", redirectCount);
        }

        res = curl_easy_getinfo(myHandle, CURLINFO_SIZE_DOWNLOAD, &size);
        
        if((CURLE_OK == res) && (size>0))
        {
          printf("Response Size: %0.0f bytes.\n", size);
        //result[no++]=val;
    }

          res = curl_easy_getinfo(myHandle, CURLINFO_SPEED_DOWNLOAD, &downSpeed);
          if((CURLE_OK == res) && (downSpeed>0))
          {

            //result[no++]=val;
            printf("Download Speed: %0.3f sec.\n",downSpeed);
        }

    long rescode;
    res = curl_easy_getinfo(myHandle, CURLINFO_RESPONSE_CODE, &rescode);
        
        if((CURLE_OK == res) && (rescode>0))
        {
          printf("Response Code: %ld\n", rescode);
          resCode=rescode;
        //result[no++]=val;
    }

        //}

  } else {
    fprintf(stderr, "Error while fetching ");
  }


    //printf("LibCurl rules!");
    curl_easy_cleanup( myHandle ); 
    
    jdouble _result[]={resCode,dns,conn,preTime,ttfb,ttime,redirectTime,redirectCount,size,downSpeed};
    (*env)->SetDoubleArrayRegion(env, result, 0 , 10, _result);
    return result;
}
