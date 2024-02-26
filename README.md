# ATI.Services.RabbitMQ
## Деплой
Выкладка в nuget происходит на основе триггера на тег определённого формата 
- `v1.0.0` - формат релизная версия на ветке master 
- `v1.0.0-rc1` - формат тестовой/альфа/бета версии на любой ветке  

Тег можно создать через git(нужно запушить его в origin) [создание тега и пуш в remote](https://git-scm.com/book/en/v2/Git-Basics-Tagging)  
или через раздел [releses](https://github.com/atidev/ATI.Services.RabbitMQ/releases)(альфа версии нужно помечать соответсвующей галкой).

#### Разработка теперь выглядит вот так:
1. Создаем ветку, пушим изменения, создаем pull request.
2. Вешаем на ветку тег например `v1.0.2-new-auth-12`
    - git tag -a v1.0.2-new-auth-12 -m "your description"  
    - git push origin v1.0.2-new-auth-12
3. Срабатывает workflow билдит и пушит версию(берёт из названия тега) в nuget.
4. По готовности мерджим ветку в master.
5. Вешаем релизный тег на нужный коммит мастера.
Нужно обязательно описать изменения внесённые этим релизом в release notes
Здесь лучше воспользоваться интерфейсом гитхаба, там удобнее редакитровать текст.
6. Срабатывает релизный workflow билдит и пушит в нугет релизную версию.
7. В разделе [Releses](https://github.com/atidev/ATI.Services.RabbitMQ/releases) появляется информация о нашем релиз и release notes.

## Документация
---
### Rabbit

Конфигурируем класс `EventbusOptions`, который должен выглядеть так:
```json 
  "EventbusOptions": {
    "ServiceName": "Service-name",
    "ConnectionString": "Rabbit-mq-connection.string",
    "Environment": "env",
    "ErrorQueueName":"error_queue_name", //очередь для хранения ошибок обработки сообщений
    "RabbitConnectInterval":"00:00:05" //время переподключения к RabbitMQ 
  }
```
В `Startup.cs` вызываем 
```c#
  services.AddEventBus();
  //или, если ChangeTrackerOptions лежат в другой секции
  services.AddEventBus(typeof(ChangeTrackerManagerOptions).Name);
```
Далее создаем свой `EventbusManager`, который отвечает за инициализацию очередей и подписок на них при старте сервиса:
```c#
 [InitializeOrder(Order = InitializeOrder.Sixth)]
 public sealed class EventbusInitializer : IInitializer
 {
   public async Task InitializeAsync()
   {
       var binding = _rmqTopology.CreateBinding(
                "your_exchange_name",
                "your_routing_key", isExclusive: false,
                isDurable: true, isAutoDelete: false);

       await _eventbusManager.SubscribeAsync(binding, Handler);
   }
   
   //this wrapped by metric collection
   private async Task Handler(byte[] message, MessageProperties properties, MessageReceivedInfo info)
   {
       var someEntityEvent = _jsonSerializer.Deserialize<SomeEntity>(message);
   }
   
    public async Task SendEmailAsync(...)
    {
        //this wrapped by metric collection
        await _eventbusManager.PublishAsync(email, "your_exchange_name", "your_routing_key", MetricEntity.SomeEntity);
    }
   
   public string InitStartConsoleMessage() => $"Start initialization for {nameof(EventbusInitializer)}";
   public string InitEndConsoleMessage() => $"End initialization for {nameof(EventbusInitializer)}";
 }
```
Регистрируем в качестве Singleton `RMQTopology`, если необходимо. 
Можно не собирать метрики при получении и отправке сообщений в вашем сервисе, т.к. это уже сделано в EventbusManager 
Готово.


