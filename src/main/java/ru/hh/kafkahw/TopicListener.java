package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

import java.util.HashSet;
import java.util.Set;

@Component
public class TopicListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;
  private final Set<Integer> receivedMessages = new HashSet<>(); // Множество для отслеживания обработанных сообщений

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  /**
   * Проблема:
   * В исходной реализации семантика AtMostOnce нарушалась, так как подтверждение (ack) отправлялось после обработки сообщения.
   * Это могло привести к повторной обработке сообщения в случае ошибки.
   *
   * Решение:
   * Подтверждение отправляется ДО обработки сообщения. Это гарантирует, что сообщение будет помечено как обработанное,
   * даже если его обработка завершится с ошибкой. Таким образом, сообщение может быть потеряно, но не обработано дважды.
   */
  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Attempting to handle message, topic: {}, payload: {}", consumerRecord.topic(), consumerRecord.value());
    ack.acknowledge(); // Подтверждение ДО обработки
    try {
      service.handle("topic1", consumerRecord.value()); // Обработка сообщения
    } catch (RuntimeException e) {
      // Исключение игнорируется, так как семантика AtMostOnce допускает потерю сообщений
      LOGGER.warn("Message lost due to exception in AtMostOnce handling, topic: {}, payload: {}",
              consumerRecord.topic(), consumerRecord.value(), e);
    }
  }

  /**
   * Проблема:
   * В исходной реализации семантика AtLeastOnce нарушалась, так как подтверждение отправлялось ДО обработки сообщения.
   * Это могло привести к потере сообщения, если обработка завершалась с ошибкой.
   *
   * Решение:
   * Подтверждение отправляется ПОСЛЕ успешной обработки сообщения. Это гарантирует, что сообщение будет обработано
   * хотя бы один раз, даже если возникнут временные ошибки.
   */
  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Attempting to handle message, topic: {}, payload: {}", consumerRecord.topic(), consumerRecord.value());
    try {
      service.handle("topic2", consumerRecord.value()); // Обработка сообщения
      ack.acknowledge(); // Подтверждение ПОСЛЕ обработки
    } catch (RuntimeException e) {
      // Исключение пробрасывается, чтобы Kafka повторно доставил сообщение
      LOGGER.error("Error handling message in AtLeastOnce, topic: {}, payload: {}",
              consumerRecord.topic(), consumerRecord.value(), e);
      throw e;
    }
  }

  /**
   * Проблема:
   * В исходной реализации семантика ExactlyOnce не была реализована, так как не отслеживались уже обработанные сообщения.
   * Это могло привести к многократной обработке одного и того же сообщения.
   *
   * Решение:
   * Добавлено множество receivedMessages для отслеживания хэшей уже обработанных сообщений.
   * Сообщение обрабатывается только один раз, даже если Kafka доставит его повторно.
   */
  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Attempting to handle message, topic: {}, payload: {}", consumerRecord.topic(), consumerRecord.value());
    int hash = consumerRecord.value().hashCode(); // Хэш сообщения для отслеживания

    if (!receivedMessages.contains(hash)) { // Проверка, было ли сообщение уже обработано
      try {
        service.handle("topic3", consumerRecord.value()); // Обработка сообщения
        ack.acknowledge(); // Подтверждение ПОСЛЕ обработки
        receivedMessages.add(hash); // Добавление хэша в множество обработанных сообщений
      } catch (RuntimeException e) {
        // Исключение пробрасывается, чтобы Kafka повторно доставил сообщение
        LOGGER.error("Error handling message in ExactlyOnce, topic: {}, payload: {}",
                consumerRecord.topic(), consumerRecord.value(), e);
        throw e;
      }
    } else {
      LOGGER.info("Message already processed, skipping. Topic: {}, payload: {}",
              consumerRecord.topic(), consumerRecord.value());
    }
  }

}
