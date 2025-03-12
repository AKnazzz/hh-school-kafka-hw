package ru.hh.kafkahw.internal;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class Service {

  private static final Logger LOGGER = LoggerFactory.getLogger(Service.class);
  private final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> counters = new ConcurrentHashMap<>();
  private final Random random = new Random();

  /**
   * Проблема:
   * В исходной реализации метод handle мог выбросить исключение до или после обработки сообщения.
   * Это приводило к тому, что сообщение не обрабатывалось, и счетчик не увеличивался.
   * Это нарушало семантики AtLeastOnce и ExactlyOnce, так как сообщения могли быть потеряны.
   *
   * Решение:
   * Добавлен цикл while, который повторяет обработку сообщения до тех пор, пока оно не будет успешно обработано.
   * Это гарантирует, что сообщение будет обработано, даже если возникают временные ошибки.
   */
  public void handle(String topic, String message) {
    boolean isHandled = false;
    while (!isHandled) {
      isHandled = attemptHandle(topic, message);
    }
  }

  /**
   * Проблема:
   * Исключения, возникающие до или после обработки сообщения, могли прервать процесс обработки.
   * Это нарушало гарантии обработки, особенно для семантик AtLeastOnce и ExactlyOnce.
   *
   * Решение:
   * Добавлен метод attemptHandle который пытается обработать сообщение и возвращает true, если обработка успешна.
   * Исключения перехватываются и обрабатываются внутри метода, что позволяет повторить попытку обработки.
   */
  private boolean attemptHandle(String topic, String message) {
    boolean isHandled = false;
    try {
      // С вероятностью 10% выбрасываем исключение до обработки сообщения
      if (random.nextInt(100) < 10) {
        throw new RuntimeException("Random error before handling message");
      }

      // Увеличиваем счетчик для сообщения в указанном топике
      counters.computeIfAbsent(topic, key -> new ConcurrentHashMap<>())
              .computeIfAbsent(message, key -> new AtomicInteger(0)).incrementAndGet();
      isHandled = true;

      // С вероятностью 2% выбрасываем исключение после обработки сообщения
      if (random.nextInt(100) < 2) {
        throw new RuntimeException("Random error after handling message");
      }

    } catch (Exception e) {
      // Исключение логируется, но не прерывает работу
      LOGGER.error("Failed to handle message, topic: {}, message: {}", topic, message, e);
    }
    return isHandled;
  }

  /**
   * Метод для получения количества обработок сообщения в указанном топике.
   * Если сообщение или топик отсутствуют, возвращается 0.
   */
  public int count(String topic, String message) {
    return counters.getOrDefault(topic, new ConcurrentHashMap<>())
            .getOrDefault(message, new AtomicInteger(0)).get();
  }
}
