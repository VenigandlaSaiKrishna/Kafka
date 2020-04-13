using Confluent.Kafka;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace KafkaProducerPoc
{
    public class ReadFileProducer
    {
        public static async Task ProcessFile()
        {
            string brokerList = "localhost:9092";
            string topicName = "topic-file-test";

            var config = new ProducerConfig { BootstrapServers = brokerList };

            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Producer {producer.Name} producing on topic {topicName}.");
                Console.WriteLine("-----------------------------------------------------------------------");
                Console.WriteLine("Ctrl-C to quit.\n");

                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                };

                using (StreamReader file = new StreamReader(@"C:\Users\user\Desktop\New folder\test.txt"))
                {
                    string data = file.ReadLine();
                    while (!string.ReferenceEquals(data, null))
                    {
                        try
                        {
                            // Note: Awaiting the asynchronous produce request below prevents flow of execution
                            // from proceeding until the acknowledgement from the broker is received (at the 
                            // expense of low throughput).
                            var deliveryReport = await producer.ProduceAsync(
                                topicName, new Message<string, string> { Key = null, Value = data });

                            Console.WriteLine($"delivered to: {deliveryReport.TopicPartitionOffset}");
                        }
                        catch (ProduceException<string, string> e)
                        {
                            Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                        }
                        data = file.ReadLine();
                    }
                }
                // Since we are producing synchronously, at this point there will be no messages
                // in-flight and no delivery reports waiting to be acknowledged, so there is no
                // need to call producer.Flush before disposing the producer.
            }
        }
    }
}



