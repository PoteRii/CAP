using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;

namespace Sample.Kafka.PostgreSql
{
    public class Startup
    {
        public const string DbConnectionString = "User ID=postgres;Password=mysecretpassword;Host=127.0.0.1;Port=5432;Database=postgres;";

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddCap(x =>
            {
                //docker run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
                x.UsePostgreSql(DbConnectionString);

                //docker run --name kafka -p 9092:9092 -d bashj79/kafka-kraft
                x.UseKafka(kafkaOptions =>
                {
                    kafkaOptions.Servers = "localhost:9092";

                    kafkaOptions.CustomHeadersBuilder = (result, provider) =>
                    {
                        List<KeyValuePair<string, string>> headers = new()
                    {
                        new("kafka.partition", result.Partition.ToString()),
                        new("kafka.offset", result.Offset.ToString())
                    };


                        return headers;
                    };

                });
                x.UseDashboard();
                x.ConsumerThreadCount = 4;
            });

            services.AddControllers();
        }

        public void Configure(IApplicationBuilder app)
        {
            app.UseRouting();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}