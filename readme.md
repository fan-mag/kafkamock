Заглушка для взаимодействия с KAFKA.

Читает из очереди, обрабатывает сообщение, записывает в другую очередь ответ.

Параметры:

kafka-input.properties - properties для брокера, с которого заглушка будет читать сообщения

Топик, из которого читать сообщения:

topic=MockIn

Как часто опрашивать очередь:

poll_interval=100

Через сколько попыток без ответа остановить заглушку:

poll_retries=500000

kafka-output.properties - properties для брокера, в который заглушка будет класть ответы

Задержка между отправкой сообщения:

pull_interval = 1

В конфигурационных файлах обязательно key(De)Serializer и value(De)Serializer матчить с (де)сериализаторами передаваемых/принимаемых объектов

<KC, VC> - generics for Input Consumer key, value

<KB, VB> - generics for Buffer

<KP, VP> - generic for Output Producer key, value

void putIntoBuffer(KC key, VC value) - обработка полученной пары <KC, VC>, запись в буффер обработанной пары <KB, VB>.

Pair<KP, VP> getFromBuffer() - обработка полученной пары <KB, VB> из буфера, добавление в выходную очередь обработанной пары <KP, VP>