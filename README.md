Nurdaulet
# student-api Bakzhuldyz

Alpakka - внутри папки есть еще 3 папки Operations , Handler , RabbitMQModel


Внутри папки Operations есть 3 основных метода : 

1)RecieveMessageAlpakka.subscription - слушать все ообщение которые поступают в определенную очередь

2)SendMessageWithCorrelationIdAlpakka.sendMessageWithCorrelationId - отправляет сообщение в определенную очередь

3)SendMessageAndWaitForResponsAlpakka.sendMessageAndWaitForResponse - Отправляет сообщение в очередь , и ждет ответа 


Внутри папки Handlers - есть встроенные специальные обработчики которые обрабатывают сообщение которые поступают в очередь или отправляються.
Тоесть он всегда слушает очередь , когда приходит сообщение , он проверяет ключ маршрутизации (routingKey) , которые мы определяем сами ,там мы можем писать допустим поиск студентов , учителей , документов итд 


