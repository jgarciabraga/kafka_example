from kafka import KafkaProducer as kp
import time
import re
import datetime

arquivo = open(r'/var/log/apache2/access.log','r')
regexp = '^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"(.+?)\"'
produtor = kp(bootstrap_servers="localhost:9092")
if __name__ == '__main__':
    while 1:
        linha = arquivo.readline()
        if not linha:
            time.sleep(5)
        else:
            x = re.match(regexp, linha).groups()
            msg = bytes(str(x), encoding='ascii')
            produtor.send('apachelog', msg)
            print(f'Mensagem Enviada em {datetime.datetime.now()}')