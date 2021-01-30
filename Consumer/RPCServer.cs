using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer
{
  class RPCServer
  {
    static void Main(string[] args)
    {
      var factory = new ConnectionFactory { HostName = "localhost" };

      using (var connection = factory.CreateConnection())
      using (var channel = connection.CreateModel())
      {
        channel.QueueDeclare(queue: "rpc_queue",
                             durable: false,
                             exclusive: false,
                             autoDelete: false,
                             arguments: null);

        channel.BasicQos(prefetchSize: 0,
                         prefetchCount: 1,
                         global: false);

        var consumer = new EventingBasicConsumer(channel);
        channel.BasicConsume(queue: "rpc_queue",
                             autoAck: false,
                             consumer);

        Console.WriteLine(" [x] Awaiting RPC requests");

        consumer.Received += (model, evenArgs) =>
        {
          var response = string.Empty;

          var body = evenArgs.Body.ToArray();
          var props = evenArgs.BasicProperties;
          var replyProps = channel.CreateBasicProperties();
          replyProps.CorrelationId = props.CorrelationId;

          try
          {
            var message = Encoding.UTF8.GetString(body);
            int n = int.Parse(message);
            Console.WriteLine(" [.] fib({0})", message);
            response = fib(n).ToString();
          }
          catch (Exception e)
          {
            Console.WriteLine(" [.] " + e.Message);
            response = "";
          }
          finally
          {
            var responseBytes = Encoding.UTF8.GetBytes(response);

            channel.BasicPublish(exchange: "",
                                 routingKey: props.ReplyTo,
                                 basicProperties: replyProps,
                                 body: responseBytes);

            channel.BasicAck(deliveryTag: evenArgs.DeliveryTag,
                             multiple: false);
          }
        };

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
      }
    }

    private static int fib(int n)
    {
      if (n == 0 || n == 1)
      {
        return n;
      }

      return fib(n - 1) + fib(n - 2);
    }
  }
}
