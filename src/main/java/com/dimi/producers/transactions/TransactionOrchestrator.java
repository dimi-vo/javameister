package com.dimi.producers.transactions;

public class TransactionOrchestrator {

    public static void main(String[] args) {

        String topic = "test";

        NonTransactionalProducer producer = new NonTransactionalProducer(10,topic);

        TransactionalProducer producerA = new TransactionalProducer(100, "producerA", topic);
        TransactionalProducer producerB = new TransactionalProducer(100, "producerB", topic);

        System.out.println("Starting both producers and sending");
        producerA.send();
        producerB.send();
        System.out.println("Done");
        producer.send();
    }
}
