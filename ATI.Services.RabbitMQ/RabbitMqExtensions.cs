using ATI.Services.Common.Behaviors;
using ATI.Services.Common.Extensions;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;

namespace ATI.Services.RabbitMQ;

public static class RabbitMqExtensions
{
    [PublicAPI]
    public static void AddEventBus(this IServiceCollection services, string? eventbusSectionName = null)
    {
        if (eventbusSectionName is null)
        {
            services.ConfigureByName<EventbusOptions>();
        }
        else
        {
            services.Configure<EventbusOptions>(ConfigurationManager.GetSection(eventbusSectionName));
        }
            
        services.AddSingleton<EventbusManager>();
        services.AddSingleton<RmqTopology>();
    }
}