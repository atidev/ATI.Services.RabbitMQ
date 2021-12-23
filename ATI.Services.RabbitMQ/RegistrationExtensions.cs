using Microsoft.Extensions.DependencyInjection;

namespace ATI.Services.RabbitMQ
{
    public static class RegistrationExtensions
    {

        public static void RegisterRmqConsumer<TService>(this IServiceCollection services) where TService : class, IRmqConsumer
        {
            services.AddSingleton<IRmqConsumer, TService>();
        }
        
        public static void RegisterRmqProducer<TService>(this IServiceCollection services) where TService : class, IRmqProducer
        {
            services.AddSingleton<TService>();
            services.AddSingleton<IRmqProducer>(f => f.GetRequiredService<TService>());
        }

        public static void RegisterRmqProducer<TService, TImplementation>(this IServiceCollection services)
            where TService : class, IRmqProducer
            where TImplementation : class, IRmqProducer, TService
        {
            services.AddSingleton<TService, TImplementation>();
            services.AddSingleton<IRmqProducer>(f => f.GetRequiredService<TService>());
        }
    }
}