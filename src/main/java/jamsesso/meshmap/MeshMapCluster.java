package jamsesso.meshmap;

import java.util.List;

public interface MeshMapCluster extends AutoCloseable {
  List<Node> getAllNodes();

  <K, V> MeshMap<K, V> join() throws MeshMapException;
}
