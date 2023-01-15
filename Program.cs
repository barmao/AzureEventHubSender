using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Text;
using System.Text.Json;

namespace EventsHubSender
{
    public class Program
    {
        // connection string to the events hubs namespace
        private const string connectionString = "<event hub connection string here>";

        // name of the event hub
        private const string eventHubName = "<event hub name her>";

        // number of events to be sent to the event hub
        private const int numOfEvents = 100;

        // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
        // of the application, which is best practice when events are being published or read regularly.
        static EventHubProducerClient producerClient;

        static async Task Main()
        {
            // Create a producer client that you can use to send events to the event hub
            producerClient = new EventHubProducerClient(connectionString, eventHubName);

            // Create a batch of events
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            var rng = new Random();


            for (int i = 1; i < numOfEvents; i++)
            {
                int lat = rng.Next(516400146, 630304598);
                int lon = rng.Next(224464416, 341194152);

                StandardCommon standardCommon = new StandardCommon()
                {
                    ClientId = rng.Next(10000, 99999).ToString(),
                    Imei = rng.NextInt64(300000000000000, 899999999999999),
                    Latitude = Convert.ToDouble(rng.Next(31,36).ToString() + "." + lat).ToString(),
                    Longitude = Convert.ToDouble(rng.Next(-1,1).ToString() + "." + lon).ToString(),
                    GPSDate = DateTime.UtcNow.AddMinutes(1).ToString(),
                    EventID = rng.Next(1, 200)
                };

                if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(standardCommon)))))
                {
                    //if it is too large for the batch
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent");
                }
            }

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"A batch of {numOfEvents} events has been published.");

            }
            finally
            {
                await producerClient.DisposeAsync();
            }

        }
    }
}