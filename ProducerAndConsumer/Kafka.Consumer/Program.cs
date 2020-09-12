using Confluent.Kafka;
using System;
using System.Threading;

namespace Kafka.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "192.168.88.48:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            using var c = new ConsumerBuilder<Ignore, string>(config).Build();
            c.Subscribe("devboost");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };
            try
            {
                while (true)
                {
                    try
                    {
                        var cr = c.Consume(cts.Token);

                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occurred: {e.Error.Reason}");

                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
            }
        }
    }
}
