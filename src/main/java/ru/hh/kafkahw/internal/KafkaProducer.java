package ru.hh.kafkahw.internal;

import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);
  private final Random random = new Random();
  private final KafkaTemplate<String, String> kafkaTemplate;

  public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  /**
   * Проблема:
   * В исходной реализации метод send мог выбросить исключение до или после отправки сообщения.
   * Это приводило к потере сообщений, что нарушало семантики AtLeastOnce и ExactlyOnce.
   *
   * Решение:
   * Добавлен цикл while, который повторяет отправку сообщения до тех пор, пока оно не будет успешно отправлено.
   * Это гарантирует, что сообщение будет доставлено, даже если возникают временные ошибки.
   */
  public void send(String topic, String payload) {
    boolean isSent = false;
    while (!isSent) {
      isSent = attemptSend(topic, payload);
    }
  }

  /**
   * Проблема:
   * Исключения, возникающие до или после отправки сообщения, могли прервать процесс отправки.
   * Это нарушало гарантии доставки, особенно для семантик AtLeastOnce и ExactlyOnce.
   *
   * Решение:
   * Добавлен метод attemptSend, который пытается отправить сообщение и возвращает true, если отправка успешна.
   * Исключения перехватываются и обрабатываются внутри метода, что позволяет повторить попытку отправки.
   */
  private boolean attemptSend(String topic, String payload) {
    boolean isSent = false;
    try {
      // С вероятностью 10% выбрасываем исключение до отправки сообщения
      if (random.nextInt(100) < 10) {
        throw new RuntimeException("Random error before sending message");
      }

      LOGGER.info("Sending message to Kafka, topic: {}, payload: {}", topic, payload);

      // Отправляем сообщение в Kafka
      kafkaTemplate.send(topic, payload);
      isSent = true;

      // С вероятностью 2% выбрасываем исключение после отправки сообщения
      if (random.nextInt(100) < 2) {
        throw new RuntimeException("Random error after sending message");
      }

    } catch (Exception e) {
      // Логируем исключение, но не прерываем процесс отправки
      LOGGER.error("Failed to send message to Kafka, topic: {}, payload: {}", topic, payload, e);
    }
    return isSent;
  }
}

