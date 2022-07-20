from kafka import KafkaConsumer as kc

if __name__ == '__main__':
    consumidor = kc(client_id='client_1', bootstrap_servers='localhost:9092',group_id='consumidores')
    consumidor.subscribe(topics=['apachelog'])
    for message in consumidor:
        print(f'Topic: {message.topic}')
        print(f'Partition: {message.partition}')
        print(f'OffSet: {message.offset}')
        print(f'Value: {message.value}')