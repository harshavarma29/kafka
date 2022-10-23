package com.kafkaproject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaService {

    private String receivedData = "Waiting";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/post-data")
    public ResponseEntity<String> getData(@RequestBody String data) {
        System.out.println("Data");
        kafkaTemplate.send("first-topic", data);
        return ResponseEntity.ok().body(receivedData);
    }

    @KafkaListener(topics = "first-topic", groupId = "first_group_id")
    public void listenData(ConsumerRecord<String, String> payload) {
        receivedData = "Data received from listener "+payload.value();
    }

}
