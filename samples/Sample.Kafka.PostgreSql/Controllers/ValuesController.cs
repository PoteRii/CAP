using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using DotNetCore.CAP;
using Microsoft.AspNetCore.Mvc;
using Npgsql;

namespace Sample.Kafka.PostgreSql.Controllers
{
    [Route("api/[controller]")]
    public class ValuesController(ICapPublisher producer) : Controller, ICapSubscribe
    {
        [Route("~/control/start")]
        public async Task<IActionResult> Start([FromServices] IBootstrapper bootstrapper)
        {
            await bootstrapper.BootstrapAsync();
            return Ok();
        }

        [Route("~/control/stop")]
        public async Task<IActionResult> Stop([FromServices] IBootstrapper bootstrapper)
        {
            await bootstrapper.DisposeAsync();
            return Ok();
        }


        [Route("~/delay/{delaySeconds:int}")]
        public async Task<IActionResult> Delay(int delaySeconds)
        {
            await producer.PublishDelayAsync(TimeSpan.FromSeconds(delaySeconds), "sample.kafka.postgrsql", DateTime.Now);

            return Ok();
        }


        [Route("~/without/transaction")]
        public async Task<IActionResult> WithoutTransaction()
        {
            await producer.PublishAsync("sample.kafka.postgrsql", DateTime.Now);

            return Ok();
        }

        [Route("~/adonet/transaction")]
        public IActionResult AdonetWithTransaction()
        {
            using (var connection = new NpgsqlConnection(Startup.DbConnectionString))
            {
                using (var transaction = connection.BeginTransaction(producer, autoCommit: false))
                {
                    //your business code
                    connection.Execute("INSERT INTO \"Persons\"(\"Name\",\"Age\",\"CreateTime\") VALUES('Lucy',25, NOW())", transaction: (IDbTransaction)transaction.DbTransaction);

                    producer.Publish("sample.kafka.postgrsql", DateTime.Now);

                    transaction.Commit();
                }
            }

            producer.Publish("sample.kafka.postgrsql", DateTime.Now);

            return Ok();
        }


        [CapSubscribe("sample.kafka.postgrsql", Group = "group1", GroupConcurrent = 1)]
        public async Task Test2(DateTime value, CancellationToken cancellationToken, [FromCap] CapHeader header)
        {
            Console.WriteLine($"Starting processing offset {header["kafka.offset"]} partition {header["kafka.partition"]}");

            Console.WriteLine("Subscriber output message: " + value);
            await Task.Delay(30000, cancellationToken);

            Console.WriteLine($"Finished processing offset {header["kafka.offset"]} partition {header["kafka.partition"]}");
        }

        [CapSubscribe("sample.kafka.postgrsql2", Group = "group1", GroupConcurrent = 1)]
        public async Task Test3(DateTime value, CancellationToken cancellationToken, [FromCap] CapHeader header)
        {
            Console.WriteLine($"Starting processing offset {header["kafka.offset"]} partition {header["kafka.partition"]}");

            Console.WriteLine("Subscriber output message: " + value);
            await Task.Delay(30000, cancellationToken);

            Console.WriteLine($"Finished processing offset {header["kafka.offset"]} partition {header["kafka.partition"]}");
        }
    }
}