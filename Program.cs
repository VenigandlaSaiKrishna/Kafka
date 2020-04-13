using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Consumer {
    class Program {
        static void Main (string[] args) {
            var config = new Dictonary<string, object> { { "group.id", "consumer-1" },
                    { "bootstrap.servers", "localhost:9092" },
                    { "enable.auto.commit", "false" }
                };

            using (var consumer = new Consumer<Null, string> (config, null, new StringDeSerializer (Encoding.UTF8))) {
                consumer.Subscribe (new string[] { "topic-1" });

                consumer.OnMessage += (_, msg) => {
                    Console.WriteLine ($"Topic: {msg.Topic}, Partition: {msg.Partition}, Offset: {msg.Offset} {msg.Value}");
                    consumer.CommitAsync (msg);
                };

                while(true){
                    consumer.Poll(100);
                }
            }
        }
    }
}