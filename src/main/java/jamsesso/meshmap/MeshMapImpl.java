package jamsesso.meshmap;

import lombok.Value;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MeshMapImpl<K, V> implements MeshMap<K, V>, MessageHandler {
  private static final String TYPE_PUT = "PUT";
  private static final String TYPE_GET = "GET";

  private final CachedMeshMapCluster cluster;
  private final MeshMapServer server;
  private final Node self;
  private final Map<Object, Object> delegate;

  public MeshMapImpl(MeshMapCluster cluster, MeshMapServer server, Node self) {
    this.cluster = new CachedMeshMapCluster(cluster);
    this.server = server;
    this.self = self;
    this.delegate = new ConcurrentHashMap<>();
  }

  @Override
  public Message handle(Message message) {
    switch (message.getType()) {
      case Message.TYPE_HI:
      case Message.TYPE_BYE: {
        cluster.clearCache();
        return Message.ACK;
      }

      case TYPE_GET: {
        Object key = message.getPayload(Object.class);
        return new Message(TYPE_GET, delegate.get(key));
      }

      case TYPE_PUT: {
        Entry entry = message.getPayload(Entry.class);
        delegate.put(entry.getKey(), entry.getValue());
        return Message.ACK;
      }

      default: {
        return Message.ACK;
      }
    }
  }

  @Override
  public int size() {
    // TODO global size
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean containsKey(Object key) {
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    return false;
  }

  @Override
  public V get(Object key) {
    Node target = getNodeForKey(key);

    if (target.equals(self)) {
      // Value is stored on the local server.
      return (V) delegate.get(key);
    }

    Message get = new Message(TYPE_GET, key);
    Message response;

    try {
      response = server.message(target, get);
    }
    catch(IOException e) {
      throw new MeshMapRuntimeException(e);
    }

    if (!TYPE_GET.equals(response.getType())) {
      throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
    }

    return (V) response.getPayload(Object.class);
  }

  @Override
  public V put(K key, V value) {
    Node target = getNodeForKey(key);

    if (target.equals(self)) {
      // Value is stored on the local server.
      return (V) delegate.put(key, value);
    }

    Message put = new Message(TYPE_PUT, new Entry(key, value));
    Message response;

    try {
      response = server.message(target, put);
    }
    catch(IOException e) {
      throw new MeshMapRuntimeException(e);
    }

    if (!Message.ACK.equals(response)) {
      throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
    }

    return value;
  }

  @Override
  public V remove(Object key) {
    return null;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {

  }

  @Override
  public void clear() {

  }

  @Override
  public Set<K> keySet() {
    return null;
  }

  @Override
  public Collection<V> values() {
    return null;
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return null;
  }

  @Override
  public void close() throws Exception {
    // TODO Transfer info to successor
  }

  private Node getNodeForKey(Object key) {
    int hash = key.hashCode() & Integer.MAX_VALUE;
    List<Node> nodes = cluster.getAllNodes();

    int leftIndex = 0;
    int rightIndex = nodes.size() - 1;

    while (true) {
      if (leftIndex == rightIndex) {
        // Only 1 element left.
        return nodes.get(leftIndex);
      }

      int midIndex = (int) Math.ceil((rightIndex + leftIndex) / 2);
      Node midNode = nodes.get(midIndex);

      // Check if this is the left-most node and the hash is less than or equal to the ID.
      if (midIndex == 0 && hash <= midNode.getId()) {
        return midNode;
      }

      // Check if this is the right-most node and the hash is greater than the ID.
      if (midIndex == nodes.size() - 1 && hash > midNode.getId()) {
        return nodes.get(0);
      }

      // Check if the object resides on the middle node.
      Node prevNode = nodes.get(midIndex - 1);

      if (hash > prevNode.getId() && hash <= midNode.getId()) {
        return midNode;
      }

      // Didn't find the target node; keep searching.
      if (hash < midNode.getId()) {
        // The target node is to the left.
        rightIndex = midIndex - 1;
      }
      else {
        // The target node is to the right.
        leftIndex = midIndex + 1;
      }
    }
  }

  @Value
  private static class Entry implements Serializable {
    Object key;
    Object value;
  }
}
