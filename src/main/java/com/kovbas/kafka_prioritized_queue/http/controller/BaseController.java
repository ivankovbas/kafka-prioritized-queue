package com.kovbas.kafka_prioritized_queue.http.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class BaseController {


    @RequestMapping(value="/", method=RequestMethod.GET)
    String root() {

        return "OK!";
    }
}
