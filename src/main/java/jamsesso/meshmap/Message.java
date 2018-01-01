package jamsesso.meshmap;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Messages have the following byte format.
 *
 * +-----------------+------------------+----------------+
 * | 32 byte type ID | 4 byte size (=X) | X byte payload |
 * +-----------------+------------------+----------------+
 */
@Data
@EqualsAndHashCode
@ToString(exclude = "payload")
public class Message {
  public static final Message HI = new Message("HI");
  public static final Message BYE = new Message("BYE");
  public static final Message ACK = new Message("ACK");
  public static final Message YES = new Message("YES");
  public static final Message NO = new Message("NO");
  public static final Message ERR = new Message("ERR");

  private static final int MESSAGE_TYPE = 32;
  private static final int MESSAGE_SIZE = 4;

  private final String type;
  private final int length;
  private final byte[] payload;

  public Message(String type) {
    this(type, 0, new byte[0]);
  }

  public Message(String type, Object payload) {
    if (type.getBytes().length > MESSAGE_TYPE) {
      throw new IllegalArgumentException("Message type must not exceed 32 bytes");
    }

    byte[] serialized = toBytes(payload);
    this.type = type;
    this.length = serialized.length;
    this.payload = serialized;
  }

  public Message(String type, byte[] payload) {
    this(type, payload.length, payload);
  }

  public Message(String type, int length, byte[] payload) {
    if (type.getBytes().length > MESSAGE_TYPE) {
      throw new IllegalArgumentException("Message type must not exceed 32 bytes");
    }

    this.type = type;
    this.length = length;
    this.payload = payload;
  }

  public <T> T getPayload(Class<T> clazz) {
    return clazz.cast(fromBytes(payload));
  }

  public int getPayloadAsInt() {
    return ByteBuffer.wrap(payload).getInt();
  }

  public void write(OutputStream outputStream) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_TYPE + MESSAGE_SIZE + length);
    byte[] typeBytes = type.getBytes();
    byte[] remainingBytes = new byte[MESSAGE_TYPE - typeBytes.length];

    buffer.put(typeBytes);
    buffer.put(remainingBytes);
    buffer.putInt(length);
    buffer.put(payload);

    outputStream.write(buffer.array());
  }

  public static Message read(InputStream inputStream) throws IOException {
    byte[] msgType = new byte[MESSAGE_TYPE];
    byte[] msgSize = new byte[MESSAGE_SIZE];

    inputStream.read(msgType);
    inputStream.read(msgSize);

    // Create a buffer for the payload
    int size = ByteBuffer.wrap(msgSize).getInt();
    byte[] msgPayload = new byte[size];

    inputStream.read(msgPayload);

    return new Message(new String(msgType).trim(), size, msgPayload);
  }

  private static byte[] toBytes(Object object) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(object);
      return bos.toByteArray();
    }
    catch(IOException e) {
      throw new MeshMapMarshallException(e);
    }
  }

  private static Object fromBytes(byte[] bytes) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
         ObjectInput in = new ObjectInputStream(bis)) {
      return in.readObject();
    }
    catch(IOException | ClassNotFoundException e) {
      throw new MeshMapMarshallException(e);
    }
  }

  private static void checkType(String type) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null");
    }

    if (type.getBytes().length > MESSAGE_TYPE) {
      throw new IllegalArgumentException("Type cannot exceed 32 bytes");
    }
  }
}
