package eu.hansolo.crac2;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import jdk.crac.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Main implements Resource {
    private static final Logger                   LOGGER = Logger.getLogger(Main.class.getName());
    private static final DateTimeFormatter        DTF    = DateTimeFormatter.ofPattern("dd.mm.YYYY HH:mm:ss");
    private              long                     counter;
    private              MqttManager              mqttManager;
    private              Runnable                 task;
    private              ScheduledExecutorService executorService;


    public Main(final Runtime runtime) throws IOException {
        LOGGER.log(Level.INFO, "Running on CRaC");
        counter     = 0;
        mqttManager = new MqttManager();
        task        = () -> {
            counter++;
            final String text = new StringBuilder().append(Constants.getNodeName()).append(" ").append(DTF.format(LocalDateTime.now())).append(" (").append(counter).append(")").toString();
            LOGGER.log(Level.INFO, text);
            mqttManager.publish(Constants.MQTT_TOPIC, MqttQos.AT_LEAST_ONCE, false, text);
        };

        runtime.addShutdownHook(new Thread(() -> {
            /* Checkpoint will be created but app will not start restoring the checkpoint
            try {
                Core.checkpointRestore();
            } catch (CheckpointException | RestoreException e) {
                LOGGER.log(Level.SEVERE, "Error creating checkpoint. " + e);
                System.out.println(e);
            }
            */
            final String text = new StringBuilder().append(Constants.getNodeName()).append(" ").append("App stopped due to shutdown hook (" + counter + ")").toString();
            LOGGER.log(Level.INFO, text);
            mqttManager.publish(Constants.MQTT_TOPIC, MqttQos.AT_LEAST_ONCE, false, text);
            mqttManager.stop();
        }));

        mqttManager.start();

        executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(task, 5, 5, TimeUnit.SECONDS);

        Core.getGlobalContext().register(Main.this);
    }


    // ******************** Methods *******************************************
    @Override public void beforeCheckpoint(Context<? extends Resource> context) throws Exception {
        LOGGER.log(Level.INFO, "beforeCheckpoint");
        executorService.shutdownNow();
        final String text = new StringBuilder().append(Constants.getNodeName()).append(" Stop before checkpoint at ").append(DTF.format(LocalDateTime.now())).toString();
        LOGGER.log(Level.INFO, text);
        mqttManager.publish(Constants.MQTT_TOPIC, MqttQos.AT_LEAST_ONCE, false, text);
        mqttManager.stop();
    }

    @Override public void afterRestore(Context<? extends Resource> context) throws Exception {
        LOGGER.log(Level.INFO, "afterRestore:");
        final String text = new StringBuilder().append(Constants.getNodeName()).append(" Started after restore at ").append(DTF.format(LocalDateTime.now())).toString();
        LOGGER.log(Level.INFO, text);
        mqttManager.start();
        mqttManager.publish(Constants.MQTT_TOPIC, MqttQos.AT_LEAST_ONCE, false, text);
        executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(task, 5,5, TimeUnit.SECONDS);
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        Runtime runtime = Runtime.getRuntime();

        Main main = new Main(runtime);
        while (true) {
            Thread.sleep(1000);
        }
    }
}
