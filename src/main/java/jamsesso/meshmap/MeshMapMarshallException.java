package jamsesso.meshmap;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class MeshMapMarshallException extends MeshMapRuntimeException {
  public MeshMapMarshallException(String msg) {
    super(msg);
  }

  public MeshMapMarshallException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public MeshMapMarshallException(Throwable cause) {
    super(cause);
  }
}
