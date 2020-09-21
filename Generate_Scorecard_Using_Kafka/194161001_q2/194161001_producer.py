import threading
from kafka import KafkaProducer
import os

host= ['localhost:9092'] 

def producer1():
    topic = '4143' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4143-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)


def producer2():
    topic = '4144' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4144-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer3():
    topic = '4145' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4145-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer4():
    topic = '4146' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4146-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer5():
    topic = '4147' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4147-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer6():
    topic = '4148' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4148-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer7():
    topic = '4149' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4149-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer8():
    topic = '4150' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4150-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer9():
    topic = '4151' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4151-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer10():
    topic = '4152' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4152-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer11():
    topic = '4153' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4153-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer12():
    topic = '4154' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4154-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer13():
    topic = '4155' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4155-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer15():
    topic = '4157' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4157-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)


def producer16():
    topic = '4158' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4158-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer17():
    topic = '4159' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4159-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer18():
    topic = '4160' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4160-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer19():
    topic = '4161' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4161-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)



def producer20():
    topic = '4162' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4162-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer21():
    topic = '4163' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4163-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer22():
    topic = '4165' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4165-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer23():
    topic = '4166' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4166-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer24():
    topic = '4168' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4168-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer25():
    topic = '4169' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4169-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer26():
    topic = '4171' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4171-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer27():
    topic = '4173' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4173-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer28():
    topic = '4174' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4174-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer29():
    topic = '4175' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4175-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer30():
    topic = '4176' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4176-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer31():
    topic = '4177' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4177-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer32():
    topic = '4178' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4178-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer33():
    topic = '4179' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4179-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer34():
    topic = '4180' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4180-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer35():
    topic = '4183' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4183-convertedcommentry.txt','r')
    for line in file.readlines():
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer36():
    topic = '4184' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4184-convertedcommentry.txt','r')
    for line in file:
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer37():
    topic = '4186' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4186-convertedcommentry.txt','r')
    for line in file:
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer38():
    topic = '4187' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4187-convertedcommentry.txt','r')
    for line in file:
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer39():
    topic = '4190' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4190-convertedcommentry.txt','r')
    for line in file:
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer40():
    topic = '4191' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4191-convertedcommentry.txt','r')
    for line in file:
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)

def producer41():
    topic = '4192' 
    producer =KafkaProducer(bootstrap_servers=host)
    file = open('194161001-4192-convertedcommentry.txt','r')
    for line in file:
        message = str.encode(line)
        producer.send(topic,b'%s' % message).get(timeout=30)


t1 = threading.Thread(target=producer1)
t2 = threading.Thread(target=producer2)
t3 = threading.Thread(target=producer3)
t4 = threading.Thread(target=producer4)
t5 = threading.Thread(target=producer5)
t6 = threading.Thread(target=producer6)
t7 = threading.Thread(target=producer7)
t8 = threading.Thread(target=producer8)
t9 = threading.Thread(target=producer9)
t10 = threading.Thread(target=producer10)
t11 = threading.Thread(target=producer11)
t12 = threading.Thread(target=producer12)
t13 = threading.Thread(target=producer13)
#t14 = threading.Thread(target=producer14)
t15 = threading.Thread(target=producer15)
t16 = threading.Thread(target=producer16)
t17 = threading.Thread(target=producer17)
t18 = threading.Thread(target=producer18)
t19 = threading.Thread(target=producer19)
t20 = threading.Thread(target=producer20)
t21 = threading.Thread(target=producer21)
t22 = threading.Thread(target=producer22)
t23 = threading.Thread(target=producer23)
t24 = threading.Thread(target=producer24)
t25 = threading.Thread(target=producer25)
t26 = threading.Thread(target=producer26)
t27 = threading.Thread(target=producer27)
t28 = threading.Thread(target=producer28)
t29 = threading.Thread(target=producer29)
t30 = threading.Thread(target=producer30)
t31 = threading.Thread(target=producer31)
t32 = threading.Thread(target=producer32)
t33 = threading.Thread(target=producer33)
t34 = threading.Thread(target=producer34)
t35 = threading.Thread(target=producer35)
t36 = threading.Thread(target=producer36)
t37 = threading.Thread(target=producer37)
t38 = threading.Thread(target=producer38)
t39 = threading.Thread(target=producer39)
t40 = threading.Thread(target=producer40)
t41 = threading.Thread(target=producer41)
#t42 = threading.Thread(target=producer42)
#t18 = threading.Thread(target=producer18)


t1.start()
t2.start()
t3.start()
t4.start()
t5.start()
t6.start()
t7.start()
t8.start()
t9.start()
t10.start()
t11.start()
t12.start()
t13.start()
#t14.start()
t15.start()
t16.start()
t17.start()
t18.start()
t19.start()
t20.start()
t21.start()
t22.start()
t23.start()
t24.start()
t25.start()
t26.start()
t27.start()
t28.start()
t29.start()
t30.start()
t31.start()
t32.start()
t33.start()
t34.start()
t35.start()
t36.start()
t37.start()
t38.start()
t39.start()
t40.start()
t41.start()
#t42.start()

#t25.start()
#t26.start()
#t18.start()
#metadata = ack.get()
#print(metadata)