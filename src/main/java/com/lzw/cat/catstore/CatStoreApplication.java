package com.lzw.cat.catstore;

import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceAutoConfigure;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.freemarker.FreeMarkerAutoConfiguration;

@SpringBootApplication(exclude = {FreeMarkerAutoConfiguration.class, DruidDataSourceAutoConfigure.class})
public class CatStoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(CatStoreApplication.class, args);
        System.out.println(11);


    }

}
