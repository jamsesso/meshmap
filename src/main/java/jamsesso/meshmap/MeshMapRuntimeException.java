package jamsesso.meshmap;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class MeshMapRuntimeException extends RuntimeException {
  public MeshMapRuntimeException(String msg) {
    super(msg);
  }

  public MeshMapRuntimeException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public MeshMapRuntimeException(Throwable cause) {
    super(cause);
  }
}
