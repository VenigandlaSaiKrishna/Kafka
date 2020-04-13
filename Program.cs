using Confluent.Kafka;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;


namespace KafkaProducerPoc
{
    public class Program : ReadFileProducer
    {
        public static async Task Main(string[] args)
        {
            await ProcessFile();
            Console.ReadLine();
        }
    }
}
