package com.learn.learnkafka.controller;

import com.learn.learnkafka.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.websocket.server.PathParam;

@RestController
@RequestMapping("/producer/{topic}")
public class ProducerController {

    @Autowired
    ProducerService producerService;

    @PostMapping(path = "/{value}/dump")
    public String producer(@PathVariable(value = "topic") String topic,
                           @PathVariable(value = "value") String value){

        producerService.produce(topic,value);

        return "Success";
    }

    @PostMapping(path = "/{key}/{value}/dump")
    public String producer(@PathVariable(value = "topic") String topic,
                           @PathVariable(value = "key") String key,
                           @PathVariable(value = "value") String value){
        producerService.produce(topic,key,value);
        return "Success";
    }
}
