package jamsesso.meshmap;

import lombok.Value;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MeshMapImpl<K, V> implements MeshMap<K, V>, MessageHandler {
  private static final String TYPE_PUT = "PUT";
  private static final String TYPE_GET = "GET";
  private static final String TYPE_REMOVE = "REMOVE";
  private static final String TYPE_CLEAR = "CLEAR";
  private static final String TYPE_KEY_SET = "KEY_SET";

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

      case TYPE_REMOVE: {
        Object key = message.getPayload(Object.class);
        return new Message(TYPE_REMOVE, delegate.remove(key));
      }

      case TYPE_CLEAR: {
        delegate.clear();
        return Message.ACK;
      }

      case TYPE_KEY_SET: {
        Object[] keys = delegate.keySet().toArray();
        return new Message(TYPE_KEY_SET, keys);
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
    return (V) get(key, getNodeForKey(key));
  }

  @Override
  public V put(K key, V value) {
    put(key, value, getNodeForKey(key));
    return value;
  }

  @Override
  public V remove(Object key) {
    return (V) remove(key, getNodeForKey(key));
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {

  }

  @Override
  public void clear() {
    Message clearMsg = new Message(TYPE_CLEAR);
    server.broadcast(clearMsg);
    delegate.clear();
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
  public String toString() {
    return "[" + String.join(", ", delegate.entrySet().stream()
      .map(entry -> entry.getKey() + ":" + entry.getValue())
      .collect(Collectors.toList()).toArray(new String[0])) + "]";
  }

  public void open() throws MeshMapException {
    Node successor = getSuccessorNode();

    // If there is no successor, there is nothing to do.
    if (successor == null) {
      return;
    }

    // Ask the successor for their key set.
    Object[] keySet = keySet(successor);

    // Transfer the keys from the successor node that should live on this node.
    List<Object> keysToTransfer = Stream.of(keySet)
      .filter(key -> {
        int hash = key.hashCode() & Integer.MAX_VALUE;

        if (self.getId() > successor.getId()) {
          // The successor is the first node (circular node list)
          return hash <= self.getId() && hash > successor.getId();
        }

        return hash <= self.getId();
      })
      .collect(Collectors.toList());

    // Store the values on the current node.
    keysToTransfer.forEach(key -> delegate.put(key, get(key, successor)));

    // Delete the keys from the remote node now that the keys are transferred.
    keysToTransfer.forEach(key -> remove(key, successor));
  }

  @Override
  public void close() throws Exception {
    Node successor = getSuccessorNode();

    // If there is no successor, there is nothing to do.
    if (successor == null) {
      return;
    }

    // Transfer the data from this node to the successor node.
    delegate.forEach((key, value) -> put(key, value, successor));
  }

  private Node getNodeForKey(Object key) {
    int hash = key.hashCode() & Integer.MAX_VALUE;
    List<Node> nodes = cluster.getAllNodes();

    for (Node node : nodes) {
      if (hash <= node.getId()) {
        return node;
      }
    }

    return nodes.get(0);
  }

  private Node getSuccessorNode() {
    List<Node> nodes = cluster.getAllNodes();

    if (nodes.size() <= 1) {
      return null;
    }

    int selfIndex = Collections.binarySearch(nodes, self, Comparator.comparingInt(Node::getId));
    int successorIndex = selfIndex + 1;

    // Find the successor node.
    if (successorIndex > nodes.size() - 1) {
      return nodes.get(0);
    }
    else {
      return nodes.get(successorIndex);
    }
  }

  private Object get(Object key, Node target) {
    if (target.equals(self)) {
      // Value is stored on the local server.
      return delegate.get(key);
    }

    Message getMsg = new Message(TYPE_GET, key);
    Message response;

    try {
      response = server.message(target, getMsg);
    }
    catch(IOException e) {
      throw new MeshMapRuntimeException(e);
    }

    if (!TYPE_GET.equals(response.getType())) {
      throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
    }

    return response.getPayload(Object.class);
  }

  private Object put(Object key, Object value, Node target) {
    if (target.equals(self)) {
      // Value is stored on the local server.
      return delegate.put(key, value);
    }

    Message putMsg = new Message(TYPE_PUT, new Entry(key, value));
    Message response;

    try {
      response = server.message(target, putMsg);
    }
    catch(IOException e) {
      throw new MeshMapRuntimeException(e);
    }

    if (!Message.ACK.equals(response)) {
      throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
    }

    return value;
  }

  private Object remove(Object key, Node target) {
    if (target.equals(self)) {
      // Value is stored on the local server.
      return delegate.remove(key);
    }

    Message removeMsg = new Message(TYPE_REMOVE, key);
    Message response;

    try {
      response = server.message(target, removeMsg);
    }
    catch(IOException e) {
      throw new MeshMapRuntimeException(e);
    }

    if (!TYPE_REMOVE.equals(response.getType())) {
      throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
    }

    return response.getPayload(Object.class);
  }

  private Object[] keySet(Node target) {
    Message keySetMsg = new Message(TYPE_KEY_SET);

    try {
      Message response = server.message(target, keySetMsg);
      return response.getPayload(Object[].class);
    }
    catch(IOException e) {
      throw new MeshMapRuntimeException(e);
    }
  }

  @Value
  private static class Entry implements Serializable {
    Object key;
    Object value;
  }
}
