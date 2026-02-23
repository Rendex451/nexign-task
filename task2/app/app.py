import os
import sys
import json
import pika

RABBIT_HOST = os.environ.get('RABBIT_HOST', 'rabbitmq')
APP_NAME = os.environ.get('APP_NAME', 'app')
INPUT_QUEUE = os.environ.get('INPUT_QUEUE')
OUTPUT_QUEUE = os.environ.get('OUTPUT_QUEUE')

def connect_to_rabbitmq():    
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        return connection
    except pika.exceptions.AMQPConnectionError as e:
        print(f"[{APP_NAME}] ОШИБКА подключения к RabbitMQ: {e}", file=sys.stderr)
        sys.exit(1)

def callback(ch, method, properties, body):
    msg = json.loads(body)
    print(f"[{APP_NAME}] Получено сообщение: {msg}")
    
    msg['trace'] = msg.get('trace', []) + [APP_NAME]
    
    if OUTPUT_QUEUE:
        ch.basic_publish(exchange='', routing_key=OUTPUT_QUEUE, body=json.dumps(msg))
        print(f"[{APP_NAME}] Отправлено дальше в очередь: {OUTPUT_QUEUE}")
    else:
        print(f"[{APP_NAME}] 🏁 ФИНАЛ. Сообщение успешно прошло весь пайплайн: {msg}")
        
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    channel.queue_declare(queue=INPUT_QUEUE, durable=True)
    if OUTPUT_QUEUE:
        channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)

    channel.basic_consume(queue=INPUT_QUEUE, on_message_callback=callback)
    
    print(f"[{APP_NAME}] Запущен. Слушаю очередь '{INPUT_QUEUE}'...")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == '__main__':
    main()
