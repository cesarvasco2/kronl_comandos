# Importa o publish do paho-mqtt
import sys
import json
import paho.mqtt.client as mqtt
import boto3
import ssl
#configurações do broker: 
Broker = 'servermqtt.duckdns.org'
PortaBroker = 1883 
Usuario = 'afira'
Senha = 'afira'
KeepAliveBroker = 60

sqs = boto3.resource('sqs', region_name='us-east-1')
queue = sqs.get_queue_by_name(QueueName='kronl_comandos')

try:
    print('[STATUS] Inicializando MQTT...') 
#inicializa MQTT:
    client = mqtt.Client()
    client.username_pw_set(Usuario, Senha)
    # the key steps here
    #context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    # if you do not want to check the cert hostname, skip it
    # context.check_hostname = False
    #client.tls_set_context(context)
    client.connect(Broker, PortaBroker, KeepAliveBroker)
    client.loop_start()
    while True:   
        for message in queue.receive_messages():
            try:
                payload = message.body
                payload_dict = json.loads(json.loads(payload))
                topic = str('GIOT-GW/DL/'+payload_dict['gateway'])     
                command_kron = json.loads('''{"macAddr":"","data":"","id":"000001","extra":{"devEUI":"","port":204,"txpara":"22"}}''')
                command_kron['macAddr'] = payload_dict['macaddr']
                command_kron['extra']['devEUI'] = payload_dict['id_dispositivo']
                if payload_dict['rele'] == 1:
                    if payload_dict['status'] == 1:
                        command_kron['data'] = '434f494c3a3033314f4e'
                    else:
                        command_kron['data'] = '434f494c3a3033314f4646'
                if payload_dict['rele'] == 2:
                    if payload_dict['status'] == 1:
                        command_kron['data'] = '434f494c3a3033324f4e'
                    else:
                        command_kron['data'] = '434f494c3a3033324f4646' 
                command_kron = json.dumps(command_kron) 
                command_kron = str('['+command_kron+']')              
                print(command_kron)
                connected = client.publish(topic,command_kron, qos=0, retain=False)
                print('\033[42;1;33m'+'Tópico: '+'\033[0;0m'+topic+ '\n\033[42;1;33m'+'Comando: '+'\033[0;0m'+command_kron)
                message.delete()
            except KeyError:
                pass    

except KeyboardInterrupt:
    print ("\nCtrl+C pressionado, encerrando aplicacao e saindo...")
    sys.exit(0)