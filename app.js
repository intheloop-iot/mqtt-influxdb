require('dotenv').config({ path: './.env' });

var mqtt = require('mqtt');
const client = mqtt.connect({
    host: process.env.MQTT_HOST,
    port: process.env.MQTT_PORT,
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
});

const mqttTopic = process.env.MQTT_TOPIC;

const {InfluxDB} = require('@influxdata/influxdb-client');
const token = process.env.DB_TOKEN;
const org = process.env.DB_ORG;
const bucket = process.env.DB_BUCKET;

const clientInfluxDB = new InfluxDB({url: 'https://eu-central-1-1.aws.cloud2.influxdata.com', token: token});

const {Point} = require('@influxdata/influxdb-client');

// MQTT Subscribe
client.on('connect', function () {
    client.subscribe(mqttTopic, function (err) {
        if (!err) {
            console.log("MQTT subscribed.")
        }
    });
});

// FETCH MQTT MESSAGES
client.on('message', function (topic, message) {
    // message is Buffer
    var payload = message.toString();
    console.log(payload);

    var payloadJSONObject = JSON.parse(payload);
    console.log(payloadJSONObject);

    var payloadSensorValue = payloadJSONObject.sensorValue;
    console.log(payloadSensorValue);
    
    // CONNECT TO INFLUXDB AND PASS THE PAYLOAD  
    const writeApi = clientInfluxDB.getWriteApi(org, bucket)
    writeApi.useDefaultTags({host: 'host1'});

    if (payloadSensorValue) {
        const pointSensorValue = new Point('sensor-value')
        .floatField('sensor', payloadSensorValue)
        writeApi.writePoint(pointSensorValue)
        writeApi
            .close()
            .then(() => {
                console.log('Sensor payload sent to InfluxDB.')
        })
            .catch(e => {
                console.error(e)
                console.log('\\nFinished ERROR')
        })
    }
 
    // client.end();
});
