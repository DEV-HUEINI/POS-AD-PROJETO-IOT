import network
import time
from machine import Pin
import dht
import ujson
from umqtt.simple import MQTTClient  # Use umqtt.simple2 para suporte a SSL

# Configurações MQTT
MQTT_CLIENT_ID = "micropython-teste-demo"
MQTT_BROKER = "broker.hivemq.com"  # URL WebSocket
MQTT_USER = "teste"
MQTT_PASSWORD = "TESTEteste@2024"
MQTT_TOPIC = "teste_topico"

# Configurações do sensor DHT22
sensor = dht.DHT22(Pin(15))

# Função para conectar ao Wi-Fi
def connect_wifi():
    print("Conectando ao WiFi...")
    sta_if = network.WLAN(network.STA_IF)
    sta_if.active(True)
    sta_if.connect('Wokwi-GUEST', '')
    while not sta_if.isconnected():
        print(".", end="")
        time.sleep(1)
    print("\nConectado ao WiFi!")


client = MQTTClient(client_id=MQTT_CLIENT_ID, server=MQTT_BROKER, user=MQTT_USER, password=MQTT_PASSWORD, ssl=True)

# Função para conectar ao servidor MQTT
def connect_mqtt():
    print("Conectando ao servidor MQTT...")
    try:
        client.connect()
        print("Conectado ao servidor MQTT!")
        return client
    except Exception as e:
        print(f"Erro ao conectar ao servidor MQTT: {e}")
        return None

# Função principal
def main():
    connect_wifi()
    client = connect_mqtt()

    if client is None:
        print("Falha na conexão MQTT. Encerrando...")
        return

    while True:
        try:
            sensor.measure()
            temperature = sensor.temperature()
            humidity = sensor.humidity()
            print(f"Temperatura: {temperature}°C, Umidade: {humidity}%")

            # Publicando os dados no tópico MQTT
            payload = ujson.dumps({"temperature": temperature, "humidity": humidity})
            client.publish(MQTT_TOPIC, payload)
            print(f"Dados publicados no tópico {MQTT_TOPIC}: {payload}")

            time.sleep(2)  # Intervalo entre as medições

        except OSError as e:
            print(f"Erro ao ler o sensor: {e}")

if __name__ == "__main__":
    main()