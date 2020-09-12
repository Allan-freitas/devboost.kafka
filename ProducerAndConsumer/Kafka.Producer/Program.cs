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

            const string bootstrapServers = "omv.serveblog.net:29092";
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
                    for (int i = 0; i < 100; i++)
                    {
                        var result = await producer.ProduceAsync(
                            nomeTopic,
                            new Message<Null, string>
                            { Value = burroCarga });

                        logger.Information(
                            $"Mensagem: {burroCarga} | " +
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

