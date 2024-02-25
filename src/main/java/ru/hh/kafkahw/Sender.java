package ru.hh.kafkahw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

@Component
public class Sender {
  private final static Logger LOGGER = LoggerFactory.getLogger(Sender.class);
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

  // Метод, который гарантирует отправку сообщения хотя бы один раз
  public void sendAtLeastOnce(String topic, String message) {
    sendAtLeastOnce(topic, message, 0);
  }

  private void sendAtLeastOnce(String topic, String message, int numOfCalls) {
    try {
      producer.send(topic, message);
    } catch (Exception ignore) {
      if (numOfCalls == 100) {
        return;
      }
      numOfCalls++;
      sendAtLeastOnce(topic, message, numOfCalls);
    }
  }
}


