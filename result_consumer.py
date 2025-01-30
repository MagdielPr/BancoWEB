# result_consumer.py
import pika
import json

class ResultConsumer:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='Fila_3', durable=True)

    def callback(self, ch, method, properties, body):
        result = json.loads(body)
        print(f" [x] Resultado do processamento: {result}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue='Fila_3', on_message_callback=self.callback)
        print(' [*] Aguardando resultados de processamento. Para sair pressione CTRL+C')
        self.channel.start_consuming()

