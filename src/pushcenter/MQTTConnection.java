package pushcenter;

import com.ibm.mqtt.*;

public class MQTTConnection implements MqttSimpleCallback {

    IMqttClient mqttClient = null;
    private static MqttPersistence MQTT_PERSISTENCE = null;
    // MQTT client ID, which is given the broker. In this example, I also use this for the topic header. 
    // You can use this to run push notifications for multiple apps with one MQTT broker. 
    public final static String MQTT_CLIENT_ID = "cocoafish";
    // We don't need to remember any state between the connections, so we use a clean start. 
    private static boolean MQTT_CLEAN_START = true;
    // Let's set the internal keep alive for MQTT to 15 mins. I haven't tested this value much. It could probably be increased.
    private static short MQTT_KEEP_ALIVE = 60 * 15;
    // Set quality of services to 0 (at most once delivery), since we don't want push notifications 
    // arrive more than once. However, this means that some messages might get lost (delivery is not guaranteed)
    private static int[] MQTT_QUALITIES_OF_SERVICE = {0};
    private static int MQTT_QUALITY_OF_SERVICE = 0;
    // The broker should not retain any messages.
    private static boolean MQTT_RETAINED_PUBLISH = false;
    private String clientID;

    // Creates a new connection given the broker address and initial topic
    public MQTTConnection(String brokerHostName, String brokerPort, String brokerClientID, String initTopic) throws MqttException {
        // Create connection spec
        String mqttConnSpec = "tcp://" + brokerHostName + "@" + brokerPort;
        // Create the client and connect
        mqttClient = MqttClient.createMqttClient(mqttConnSpec, MQTT_PERSISTENCE);
        this.clientID = brokerClientID;
        mqttClient.connect(clientID, MQTT_CLEAN_START, MQTT_KEEP_ALIVE);

        // register this client app has being able to receive messages
        mqttClient.registerSimpleHandler(this);

        // Subscribe to an initial topic, which is combination of client ID and device ID.
        initTopic = MQTT_CLIENT_ID + "/" + initTopic;
        subscribeToTopic(initTopic);

        System.out.println("Connection established to " + brokerHostName + " on topic " + initTopic);
    }
    
    public boolean isConnected(){
        return mqttClient.isConnected();
    }

    // Disconnect
    public void disconnect() {
        try {
            mqttClient.disconnect();
        } catch (MqttPersistenceException e) {
            System.err.println("MqttException" + (e.getMessage() != null ? e.getMessage() : " NULL"));
        }
    }
    /*
     * Send a request to the message broker to be sent messages published with 
     *  the specified topic name. Wildcards are allowed.	
     */

    private void subscribeToTopic(String topicName) throws MqttException {

        if ((mqttClient == null) || (mqttClient.isConnected() == false)) {
            // quick sanity check - don't try and subscribe if we don't have
            //  a connection
            System.err.println("Connection error" + "No connection");
        } else {
            String[] topics = {topicName};
            mqttClient.subscribe(topics, MQTT_QUALITIES_OF_SERVICE);
        }
    }
    /*
     * Sends a message to the message broker, requesting that it be published
     *  to the specified topic.
     */

    public void publishToTopic(String topicName, String message) throws MqttException {
        if ((mqttClient == null) || (mqttClient.isConnected() == false)) {
            // quick sanity check - don't try and publish if we don't have
            //  a connection				
            System.out.println("No connection to public to");
        } else {
            mqttClient.publish(MQTT_CLIENT_ID + "/" + topicName,
                    message.getBytes(),
                    MQTT_QUALITY_OF_SERVICE,
                    MQTT_RETAINED_PUBLISH);
            System.out.println("Publish sucessfully");
        }
    }

    /*
     * Called if the application loses it's connection to the message broker.
     */
    public void connectionLost() throws Exception {
        System.out.println("Loss of connection" + "connection downed");
        mqttClient.connect(clientID, MQTT_CLEAN_START, MQTT_KEEP_ALIVE);
    }

    /*
     * Called when we receive a message from the message broker. 
     */
    public void publishArrived(String topicName, byte[] payload, int qos, boolean retained) {
        // Show a notification
        String s = new String(payload);
        //showNotification(s);
        System.out.println("Got message: " + s);
    }
//    public void sendKeepAlive() throws MqttException {
//        System.out.println("Sending keep alive");
//        // publish to a keep-alive topic
//        publishToTopic(MQTT_CLIENT_ID + "/keepalive", mPrefs.getString(PREF_DEVICE_ID, ""));
//    }
}
