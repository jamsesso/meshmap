package jamsesso.meshmap;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class MeshMapException extends Exception {
  public MeshMapException(String msg) {
    super(msg);
  }

  public MeshMapException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public MeshMapException(Throwable cause) {
    super(cause);
  }
}
