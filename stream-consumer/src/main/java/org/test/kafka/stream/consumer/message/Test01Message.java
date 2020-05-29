package org.test.kafka.stream.consumer.message;

public class Test01Message {
    private int id;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Test01Message{" +
                "id=" + id +
                '}';
    }
}
