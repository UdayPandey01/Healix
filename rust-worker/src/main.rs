use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Deserialize, Serialize, Debug)]
struct AlertPayload {
    tenant_id: String,
    alert_id: String,
    service_name: String,
    error_message: String,
    stack_trace: Option<String>,
}

#[tokio::main]
async fn main() {
    println!("Starting Healix Event Router...");

    let kafka_brokers = "healix-kafka:9092";
    let topic = "INBOUND_ALERTS";

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "healix-router-group")
        .set("bootstrap.servers", kafka_brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Failed to create Kafka consumer");

    consumer.subscribe(&[topic]).expect("Failed to subscribe to topic");
    
    let redis_url = "redis://healix-redis:6379";
    let redis_client = redis::Client::open(redis_url).expect("Invalid Redis URL");
    
    let redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect to Redis");

    println!("Connected to Kafka & Redis. Listening on topic: {}", topic);

    loop {
        match consumer.recv().await {
            Err(e) => {
                eprintln!("Kafka error: {}", e);
            }
            Ok(m) => {
                let payload_str = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        eprintln!("Deserialization error: {:?}", e);
                        ""
                    }
                };

                match serde_json::from_str::<AlertPayload>(payload_str) {
                    Ok(alert) => {
                        println!("Parsed Alert [{}] for Tenant [{}]", alert.alert_id, alert.tenant_id);

                        let queue_key = format!("queue:tenant:{}", alert.tenant_id);
                        
                        let clean_json = serde_json::to_string(&alert).unwrap();
                        
                        let mut conn = redis_conn.clone();
                        
                        match conn.lpush::<_, _, i64>(&queue_key, clean_json).await {
                            Ok(queue_length) => {
                                println!("Pushed to Redis '{}' (Queue length: {})", queue_key, queue_length);
                            }
                            Err(e) => {
                                eprintln!("Failed to push to Redis: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Invalid Payload Format. Dropping message. Error: {}", e);
                    }
                }
            }
        };
    }
}