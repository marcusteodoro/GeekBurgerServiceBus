using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BurgerGeekServiceBus
{
    class Program
    {

        const string QueueConnectionString = "Endpoint=sb://geekburgermarcus.servicebus.windows.net/;SharedAccessKeyName=ProductPolicy;SharedAccessKey=ZRXhY7r3mw2jmlyDduRjwJUHhjLSs4GqhLS1fdZmB8U=;";
        const string QueuePath = "productchanged";
        static IQueueClient _queueClient;
        static List<Task> PendingCompleteTasks = new List<Task>();
        private static void Main(string[] args)
        {
            if (args.Length <= 0 || args[0] == "sender")
            {
                SendMessagesAsync().GetAwaiter().GetResult();
                Console.WriteLine("messages were sent");
            }
            else if (args[0] == "receiver")
            {
                ReceiveMessagesAsync().GetAwaiter().GetResult();
                Console.WriteLine("messages were received");
            }
            else
                Console.WriteLine("nothing to do");

            Console.ReadLine();


            SendMessagesAsync().GetAwaiter().GetResult();
            Console.WriteLine("messages were sent");
            Console.ReadLine();
        }

        private static async Task SendMessagesAsync()
        {
            var queueClient = new QueueClient(QueueConnectionString, QueuePath);
            queueClient.OperationTimeout = TimeSpan.FromSeconds(10);
            var messages = " Hi,Hello,Hey,How are you,Be Welcome"
                .Split(',')
                .Select(msg =>
                {
                    Console.WriteLine($"Will send message: {msg}");
                    return new Message(Encoding.UTF8.GetBytes(msg));
                })
                .ToList();
            var sendTask = queueClient.SendAsync(messages);
            await sendTask;
            CheckCommunicationExceptions(sendTask);
            var closeTask = _queueClient.CloseAsync();
            await closeTask;
            CheckCommunicationExceptions(closeTask);
        }


        private static async Task ReceiveMessagesAsync()
        {
            _queueClient = new QueueClient(QueueConnectionString, QueuePath, ReceiveMode.PeekLock);
            _queueClient.RegisterMessageHandler(MessageHandler, new MessageHandlerOptions(ExceptionHandler) { AutoComplete = false });
            Console.ReadLine();
            Console.WriteLine($" Request to close async. Pending tasks: { PendingCompleteTasks.Count}");
            await Task.WhenAll(PendingCompleteTasks);
            Console.WriteLine($"All pending tasks were completed");
            var closeTask = _queueClient.CloseAsync();
            await closeTask;
            CheckCommunicationExceptions(closeTask);
        }


        private static Task ExceptionHandler(ExceptionReceivedEventArgs exceptionArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionArgs.Exception}.");
            var context = exceptionArgs.ExceptionReceivedContext;
            Console.WriteLine($"Endpoint:{context.Endpoint}, Path:{context.EntityPath}, Action:{context.Action}");
            return Task.CompletedTask;
        }

        private static async Task MessageHandler(Message message, CancellationToken cancellationToken)
        {
            Console.WriteLine($"Received message{ Encoding.UTF8.GetString(message.Body)}");

            if (cancellationToken.IsCancellationRequested || _queueClient.IsClosedOrClosing)
                return;
            var count = 1;
            Console.WriteLine($"task {count++}");
            Task PendingTask;
            lock (PendingCompleteTasks)
            {
                PendingCompleteTasks.Add(_queueClient.CompleteAsync(message.SystemProperties.LockToken));
                PendingTask = PendingCompleteTasks.LastOrDefault();
            }
            Console.WriteLine($"calling complete for task {count}");
            await PendingTask;
            Console.WriteLine($"remove task {count} from task queue");
            PendingCompleteTasks.Remove(PendingTask);
        }


        public static bool CheckCommunicationExceptions(Task task)
        {
            if (task.Exception == null || task.Exception.InnerExceptions.Count == 0) return true;

            task.Exception.InnerExceptions.ToList()
                .ForEach(innerException =>
                {
                    Console.WriteLine($"Error in SendAsync task:{ innerException.Message}.Details: { innerException.StackTrace}");

                    if (innerException is ServiceBusCommunicationException)
                        Console.WriteLine("Connection Problem with Host");
                });

            return false;
        }
    }
}
