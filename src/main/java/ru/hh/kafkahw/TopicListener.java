package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  // При падении обработчика в @KafkaListener, сообщение все равно обрабатывается повторно.
  // Чтобы этого избежать, явно ловим ошибку и вызываем acknowledge().
  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    try {
      LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
      service.handle("topic1", consumerRecord.value());
    }
    catch (RuntimeException e) {
      LOGGER.error("Error while handling message", e);
    }
    finally {
      ack.acknowledge();
    }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    service.handle("topic2", consumerRecord.value());
    ack.acknowledge();
  }

  //Обрабатываем сообщение, только если раньше у него не было успешных обработок.
  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    if (service.count(consumerRecord.topic(), consumerRecord.value()) == 0) {
      service.handle("topic3", consumerRecord.value());
    };
    ack.acknowledge();
  }
}
