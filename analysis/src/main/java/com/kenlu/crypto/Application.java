package com.kenlu.crypto;

import com.kenlu.crypto.config.AppConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Application {

    public static void main(String[] args) {
        ApplicationContext ctx = new AnnotationConfigApplicationContext(AppConfig.class);
        TaskRunner taskRunner = ctx.getBean(TaskRunner.class);

        taskRunner.run();
    }

}
