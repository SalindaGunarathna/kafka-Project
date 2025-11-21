package com.consumer.orders_consumer_service.service;

import com.salinda.kafka.avro.Order;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderConsumerService {

    private double totalPriceSum = 0.0;
    private int totalOrderCount = 0;

    @KafkaListener(topics = "${application.kafka.topic}", groupId = "orders-group")
    public void consumeOrder(ConsumerRecord<String, Order> record) {
        Order order = record.value();
        double price = order.getPrice();


        synchronized (this) {
            totalOrderCount++;
            totalPriceSum += price;

            // 3. Calculate Average
            double averagePrice = totalPriceSum / totalOrderCount;


            System.out.printf(" New Order: %s ($%.2f) | Total Orders: %d |  Running Average: $%.2f%n",
                    order.getProduct(), price, totalOrderCount, averagePrice);
        }
    }
}