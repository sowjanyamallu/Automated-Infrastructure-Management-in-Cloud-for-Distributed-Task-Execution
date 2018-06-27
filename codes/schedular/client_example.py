import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='tasks')
channel.basic_publish(exchange='',
                      routing_key='tasks',
                      body='convert https://storage.googleapis.com/vnagpure-videos/Hotel%20California.mp4')
print(" [x] Sent 'Hello World!'")
