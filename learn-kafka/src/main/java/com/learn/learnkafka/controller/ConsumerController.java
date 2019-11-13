package com.learn.learnkafka.controller;

import com.learn.learnkafka.service.ConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/consumer/{group}/{topic}")
public class ConsumerController {

    @Autowired
    ConsumerService consumerService;

    public String consume(@PathVariable (value = "group") String group,
                          @PathVariable (value = "topic") String topic){
        return consumerService.consume(group,topic);
    }

}
