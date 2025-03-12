package ru.hh.kafkahw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

@Component
public class Sender {
  private final KafkaProducer producer;

  @Autowired
  public Sender(KafkaProducer producer) {
    this.producer = producer;
  }

  public void doSomething(String topic, String message) {
    try {
      producer.send(topic, message);
    } catch (Exception ignore) {
    }
  }
}

