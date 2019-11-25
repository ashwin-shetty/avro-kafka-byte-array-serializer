package com.ashu.kafkaavro;

import com.ashu.Employee;
import lombok.extern.apachecommons.CommonsLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {

  private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Producer.class);

  @Value("${topic.name}")
  private String topic;

  private final KafkaTemplate<String, byte[]> kafkaTemplate;

  @Autowired
  public Producer(KafkaTemplate<String, byte[]> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  void sendMessage(Employee employee) {
    byte[] employeeData = KafkaEncoder.encode(employee);
    this.kafkaTemplate.send(this.topic, employee.getName(), employeeData);
    log.info(String.format("Produced user -> %s", employee));
  }
}