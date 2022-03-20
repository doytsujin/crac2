package eu.hansolo.crac2;

public class Constants {
    private Constants() {}

    public static final String NODE_NAME = "NODE_NAME";
    public static final String getNodeName() {
        final String envVariable = System.getenv(NODE_NAME);
        if (null == envVariable) {
            System.out.println("Environment variable " + NODE_NAME + " not found, default to CRaC2-Demo");
            return "CRaC2-Demo";
        } else {
            return envVariable;
        }
    }

    public static final String MQTT_HOST       = "efd8372d97e04f0ea535ff64484f5902.s2.eu.hivemq.cloud";
    public static final int    MQTT_PORT       = 8333;
    public static final String MQTT_USER       = "crac1";
    public static final String MQTT_PW         = "OpenJDK0nCrac";
    public static final String MQTT_TOPIC      = "crac";
    public static final String MQTT_ONLINE_MSG = getNodeName() + " went online";
    public static final String MQTT_LAST_WILL  = getNodeName() + " went offline";
}
