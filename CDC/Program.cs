using CDC;
using IntegrationEventLogEF.Configuration;
using RabbitMQEventBus.Configuration;
using Serilog;
using Serilog.Events;


IHost host = Host.CreateDefaultBuilder(args)
    .UseSerilog()
    .ConfigureServices((hostContext, services) =>
    {
        services.ConfigureRabbitMQEventBus();
        services.ConfigureIntegrationEventLog(hostContext.Configuration);

        services.AddHostedService<Worker>();
    })
    .Build();


Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

try
{
    Log.Information("Starting CDC");
    await host.RunAsync();
    Environment.Exit(0);
}
catch (Exception ex)
{
    Log.Fatal(ex, "Host terminated unexpectedly");
    Environment.Exit(1);
}
finally
{
    Log.CloseAndFlush();
}