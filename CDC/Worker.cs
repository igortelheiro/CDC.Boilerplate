using EventBus.Core.Events;
using EventBus.Core.Interfaces;
using IntegrationEventLogEF;
using IntegrationEventLogEF.Services;
using IntegrationEventLogEF.Utilities;

namespace CDC;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IServiceProvider _serviceProvider;

    private IIntegrationEventLogService _eventLogService;
    private IntegrationEventLogContext _dbContext;
    private IEventBus _bus;

    private const int PublishRetryLimit = 5;
    private const int PoolingDelayInMinutes = 2;

    public Worker(ILogger<Worker> logger, IServiceProvider serviceProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await ExecuteJobAsync();
            await Task.Delay(TimeSpan.FromMinutes(PoolingDelayInMinutes), stoppingToken);
        }
    }


    private async Task ExecuteJobAsync()
    {
        try
        {
            _logger.LogInformation("Executando Job");

            using (var scope = _serviceProvider.CreateScope())
            {
                _eventLogService = scope.ServiceProvider.GetRequiredService<IIntegrationEventLogService>();
                _dbContext = scope.ServiceProvider.GetRequiredService<IntegrationEventLogContext>();
                _bus = scope.ServiceProvider.GetRequiredService<IEventBus>();

                var events = await RetriveEventsPendingToPublishAsync();
                await PublishPendingEventsAsync(events);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha ao executar Job");
        }
        finally
        {
            _logger.LogInformation("Próxima execução: " + DateTime.Now.AddMinutes(PoolingDelayInMinutes).ToShortTimeString());
        }
    }


    private async Task<IEnumerable<IntegrationEventLogEntry>> RetriveEventsPendingToPublishAsync()
    {
        var pendingEvents = await _eventLogService.RetrieveEventLogsPendingToPublishAsync();

        var eventsFailedToPublish = (await _eventLogService.RetrieveEventLogsFailedToPublishAsync())
            .Where(e => e.TimesSent < PublishRetryLimit);

        IEnumerable<IntegrationEventLogEntry> events = Array.Empty<IntegrationEventLogEntry>();

        return events.Concat(pendingEvents).Concat(eventsFailedToPublish);
    }


    private async Task PublishPendingEventsAsync(IEnumerable<IntegrationEventLogEntry> events)
    {
        foreach (var @event in events)
        {
            try
            {
                var nestedType = @event.IntegrationEvent.GetType();
                await ResilientTransaction.New(_dbContext).ExecuteAsync(async cancellationToken =>
                {
                    await _bus.PublishAsync(@event.IntegrationEvent, cancellationToken);
                    await _eventLogService.MarkEventAsPublishedAsync(@event.EventId, cancellationToken);
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Erro ao publicar evento {@event.EventTypeShortName} - TransactionId: {@event.TransactionId}");

                var cancellationToken = new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token;
                await _eventLogService.MarkEventPublishAsFailedAsync(@event.EventId, cancellationToken);
            }
        }
    }
}
