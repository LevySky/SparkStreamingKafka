package com.java;
import com.scala.StreamManager;
//import com.spark.streaming.StremHandle;

//
//import com.spark.streaming.StremApplication;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.context.ConfigurableApplicationContext;
//
//@SpringBootApplication
public class Application {

    public static void main(String[] args) throws Exception{

       // StremHandle.handle();
      //  StreamManager.start10();
        StreamManager.initByZK();
        //HandleHelper.test();
//        ConfigurableApplicationContext cac = SpringApplication.run(Application.class, args);
//        if(cac.isRunning()){
//            StremApplication.handle();
//        }
    }
}
