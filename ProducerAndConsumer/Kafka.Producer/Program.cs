using Confluent.Kafka;
using Serilog;
using Serilog.Core;
using System;
using System.Threading.Tasks;

namespace Kafka.Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Logger logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            logger.Information("Testando o envio de mensagens com Kafka");

            const string bootstrapServers = "192.168.88.48:9092";
            const string nomeTopic = "devboost";
            string burroCarga = $"Mensagem => {Guid.NewGuid()} Hora => {DateTime.Now}";

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");

            try
            {
                ProducerConfig config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                };

                using (IProducer<Null, string> producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    for (int i = 2; i < args.Length; i++)
                    {
                        var result = await producer.ProduceAsync(
                            nomeTopic,
                            new Message<Null, string>
                            { Value = burroCarga });

                        logger.Information(
                            $"Mensagem: {args[i]} | " +
                            $"Status: { result.Status}");
                    }
                }

                logger.Information("Jegue/Burro despachado");
                Console.WriteLine("Jegue/Burro despachado");
            }
            catch (Exception ex)
            {
                throw ex;
            }

            Console.ReadKey();
        }
    }
}

