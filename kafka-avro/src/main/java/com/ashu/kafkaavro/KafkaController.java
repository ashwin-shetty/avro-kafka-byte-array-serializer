package com.ashu.kafkaavro;

import com.ashu.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping(value = "/user")
public class KafkaController {

  private final Producer producer;

  @Autowired
  KafkaController(Producer producer) {
    this.producer = producer;
  }

  @PostMapping(value = "/publish")
  public void sendMessageToKafkaTopic(@RequestParam("id") Integer id,@RequestParam("name") String name, @RequestParam("emailid") String emailid) {
    this.producer.sendMessage(new Employee(id, name, emailid));
  }
}