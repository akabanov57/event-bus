package av.kabanov.event;

import av.kabanov.internal.UniversalEventAdmin;

public final class EventAdminFactory {

  /**
   * Создает экземпляр шины со стандартным выводом ошибок в System.err.
   */
  public static EventAdmin create() {
    return new UniversalEventAdmin(null);
  }

  /**
   * Создает экземпляр шины с кастомной стратегией обработки исключений.
   */
  public static EventAdmin create(EventExceptionHandler exceptionHandler) {
    return new UniversalEventAdmin(exceptionHandler);
  }

}
