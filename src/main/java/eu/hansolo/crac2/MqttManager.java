package eu.hansolo.crac2;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;

import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;


public class MqttManager {
    private static final Logger              LOGGER = Logger.getLogger(MqttManager.class.getName());
    private              Mqtt5BlockingClient client;


    // ******************** Constructors **************************************
    public MqttManager() {

    }


    // ******************** Methods *******************************************
    public void start() {
        if (null == client) {
            try {
                client = createClient();
            } catch (NullPointerException e) {
                LOGGER.log(Level.SEVERE, "Error connecting to MQTT broker " + Constants.MQTT_HOST + " on port 8883. " + e.getMessage());
            }
        }
        connect(true);
    }

    public void stop() {
        if (null == client) { return; }
        client.disconnect();
    }

    public Mqtt5PublishResult publish(final String topic, final String msg) {
        return publish(topic, MqttQos.EXACTLY_ONCE, false, msg);
    }
    public Mqtt5PublishResult publish(final String topic, final MqttQos qos, final boolean retain, final String msg) {
        return client.publishWith()
                     .topic(topic)
                     .payload(UTF_8.encode(msg))
                     .send();
    }

    public void connect(final boolean cleanStart) {
        if (null == client) {
            try {
                client = createClient();
            } catch (NullPointerException e) {
                LOGGER.log(Level.SEVERE, "Error connecting to MQTT broker " + Constants.MQTT_HOST + " on port 8883. " + e.getMessage());
            }
        }

        client.connectWith()
              .simpleAuth()
              .username(Constants.MQTT_USER)
              .password(UTF_8.encode(Constants.MQTT_PW))
              .applySimpleAuth()
              .send();
    }

    private Mqtt5BlockingClient createClient() {
        final Mqtt5BlockingClient client = MqttClient.builder()
                                                     .useMqttVersion5()
                                                     .serverHost(Constants.MQTT_HOST)
                                                     .serverPort(8883)
                                                     .sslWithDefaultConfig()
                                                     .buildBlocking();
        return client;
    }
}
