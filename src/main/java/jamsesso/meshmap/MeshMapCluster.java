package jamsesso.meshmap;

import java.util.List;

public interface MeshMapCluster {
  List<Node> getAllNodes();

  <K, V> MeshMap<K, V> join() throws MeshMapException;
}
