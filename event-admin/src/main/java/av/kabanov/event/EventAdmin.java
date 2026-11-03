package av.kabanov.event;

import av.kabanov.internal.UniversalEventAdmin;
import java.util.function.Consumer;

/**
 * Публичный контракт и фабрика для работы с универсальной шиной событий. Содержит в себе скрытую
 * потокобезопасную реализацию на виртуальных потоках Java 25.
 * <p>
 * Архитектура полностью защищена от утечек памяти, поддерживает контроль жизненного цикла и
 * предотвращает любые операции после закрытия (close).
 */
public interface EventAdmin extends AutoCloseable {

  /**
   * Подписка на топик с автоматической проверкой типа события.
   *
   * @param topicPattern маска топика (например, "orders/new" или "orders/*")
   * @param eventClass   класс события для фильтрации
   * @param listener     обработчик события
   * @return {@link Subscriber} токен отмены подписки, не выбрасывающий Exception при закрытии.
   * @throws IllegalStateException если шина событий уже закрыта
   */
  <E extends Event> Subscriber subscribe(String topicPattern, Class<E> eventClass,
      Consumer<E> listener);

  /**
   * Асинхронная публикация события в пул виртуальных потоков (без блокировки продюсера). Защищена
   * встроенным механизмом Backpressure.
   *
   * @throws IllegalStateException если шина событий уже закрыта
   */
  void postEvent(Event event);

  // --- Фабричные методы ---

  /**
   * Создает экземпляр шины со стандартным выводом ошибок в System.err.
   */
  static EventAdmin create() {
    return new UniversalEventAdmin(null);
  }

  /**
   * Создает экземпляр шины с кастомной стратегией обработки исключений.
   */
  static EventAdmin create(EventExceptionHandler exceptionHandler) {
    return new UniversalEventAdmin(exceptionHandler);
  }

  /**
   * Переопределяем метод close() из AutoCloseable, убирая 'throws Exception'. Теперь шину можно
   * безопасно закрывать в try-with-resources без перехвата исключений.
   */
  @Override
  void close();
}

