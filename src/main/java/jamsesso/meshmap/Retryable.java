package jamsesso.meshmap;

import java.util.stream.Stream;

public final class Retryable<T> {
  private final Task<T> task;
  private Class<? extends Exception>[] causes;

  private Retryable(Task<T> task) {
    this.task = task;
  }

  public static <T> Retryable<T> retry(Task<T> task) {
    return new Retryable<>(task);
  }

  @SafeVarargs
  public final Retryable<T> on(Class<? extends Exception>... causes) {
    this.causes = causes;
    return this;
  }

  public final T times(int times) throws Exception {
    // Performs the action times-1 times.
    for (int i = 1; i < times; i++) {
      try {
        return task.apply();
      }
      catch (Exception e) {
        boolean shouldRetry = Stream.of(causes).anyMatch(cause -> cause.isInstance(e));

        if (!shouldRetry) {
          throw e;
        }
      }
    }

    // Last try.
    return task.apply();
  }

  public interface Task<T> {
    T apply() throws Exception;
  }
}
