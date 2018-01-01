package jamsesso.meshmap;

@FunctionalInterface
public interface MessageHandler {
  Message handle(Message message);
}
