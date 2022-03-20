package eu.hansolo.crac2;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5WillPublish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.hivemq.client.mqtt.MqttGlobalPublishFilter.ALL;
import static java.nio.charset.StandardCharsets.UTF_8;


public class MqttManager {
    private static final Logger           LOGGER = Logger.getLogger(MqttManager.class.getName());
    private              Mqtt5AsyncClient asyncClient;
    private              AtomicBoolean    running;


    // ******************** Constructors **************************************
    public MqttManager() {
        this.running = new AtomicBoolean(Boolean.TRUE);
    }


    // ******************** Methods *******************************************
    public void start() {
        if (null == asyncClient) {
            try {
                asyncClient = createAsyncClient();
            } catch (NullPointerException e) {
                LOGGER.log(Level.SEVERE, "Error connecting to MQTT broker " + Constants.MQTT_HOST + " on port " + Constants.MQTT_PORT + ". " + e.getMessage());
            }
        }
        connect(true);
    }

    public void stop() {
        if (null == asyncClient) { return; }
        asyncClient.disconnect();
        running.set(false);
    }


    public CompletableFuture<Mqtt5PublishResult> publish(final String topic, final String msg) {
        return publish(topic, MqttQos.EXACTLY_ONCE, false, msg);
    }
    public CompletableFuture<Mqtt5PublishResult> publish(final String topic, final MqttQos qos, final boolean retain, final String msg) {
        if (null == asyncClient || !asyncClient.getState().isConnected()) {
            start();
        }
        return asyncClient.publishWith()
                          .topic(topic)
                          .qos(qos)
                          .retain(retain)
                          .payload(UTF_8.encode(msg))
                          .send()
                          .whenComplete((publish, throwable) -> {
                              if (throwable != null) {
                                  // Handle failure to publish
                                  LOGGER.log(Level.SEVERE, "Error sending mqtt msg to " + topic + " with content " + msg + ", error " + throwable.getMessage());
                                  asyncClient.disconnect();
                              } else {
                                  // Handle successful publish, e.g. logging or incrementing a metric
                              }
                          });
    }

    public CompletableFuture<Mqtt5SubAck> subscribe(final String topic, final MqttQos qos) {
        if (null == asyncClient || !asyncClient.getState().isConnected()) { connect(true); }
        return asyncClient.subscribeWith()
                          .topicFilter(topic)
                          .qos(qos)
                          //.callback(publish -> {
                          //    if (publish.getPayload().isPresent()) {
                          //        fireMqttEvent(new MqttEvt(publish.getTopic().toString(), new String(publish.getPayloadAsBytes())));
                          //    }
                          //})
                          .send();
    }

    public void unsubscribe(final String topic) {
        if (null == asyncClient || !asyncClient.getState().isConnected()) { connect(true); }
        asyncClient.unsubscribeWith().topicFilter(topic).send();
    }

    public void connect(final boolean cleanStart) {
        if (null == asyncClient) {
            try {
                asyncClient = createAsyncClient();
            } catch (NullPointerException e) {
                LOGGER.log(Level.SEVERE, "Error connecting to MQTT broker " + Constants.MQTT_HOST + " on port " + Constants.MQTT_PORT + ". " + e.getMessage());
            }
        }

        if (!asyncClient.getState().isConnectedOrReconnect() && MqttClientState.CONNECTING != asyncClient.getState() && MqttClientState.CONNECTING_RECONNECT != asyncClient.getState()) {
            asyncClient.connectWith()
                       .cleanStart(cleanStart)
                       .sessionExpiryInterval(0)
                       .keepAlive(30)
                       .simpleAuth()
                       .username(Constants.MQTT_USER)
                       .password(UTF_8.encode(Constants.MQTT_PW))
                       .applySimpleAuth()
                       .willPublish(Mqtt5WillPublish.builder()
                                                    .topic(Constants.MQTT_TOPIC)
                                                    .qos(MqttQos.EXACTLY_ONCE)
                                                    .payload(Constants.MQTT_LAST_WILL.getBytes(UTF_8))
                                                    .build())
                       .send()
                       .whenComplete((connAck, throwable) -> {
                           if (throwable != null) {
                               // Handle failure to publish
                               LOGGER.log(Level.SEVERE, "Error connecting to mqtt broker " + throwable.getMessage());
                           } else {
                               // Handle successful publish, e.g. logging or incrementing a metric
                               publish(Constants.MQTT_TOPIC, MqttQos.EXACTLY_ONCE,true, Constants.MQTT_ONLINE_MSG);
                           }
                           running.set(null == throwable);
                       });
        }
        asyncClient.publishes(ALL, publish -> {
            if (publish.getPayload().isPresent()) {

            }
        });
    }


    private Mqtt5AsyncClient createAsyncClient() {
        Mqtt5AsyncClient asyncClient = MqttClient.builder()
                                                 .useMqttVersion5()
                                                 .serverHost(Constants.MQTT_HOST)
                                                 .serverPort(Constants.MQTT_PORT)
                                                 .sslWithDefaultConfig()
                                                 .addDisconnectedListener(context -> {
                                                     /*
                                                     if (running.get()) {
                                                         context.getReconnector().reconnect(true).delay(10, TimeUnit.SECONDS);
                                                         LOGGER.log(Level.INFO, "Try to reconnect because of: " + context.getCause().getMessage());
                                                     }
                                                     */
                                                 })
                                                 .buildAsync();
        return asyncClient;
    }
}
