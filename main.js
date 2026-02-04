const express = require('express')
const local_setting = require("./Local_settings.json");
const app = express()
const port = local_setting.Request_port;

const redis = require('redis');
const client = redis.createClient({
    url : "redis://" + local_setting.Redis_IP + ":" + local_setting.Redis_port
});

client.on('error', err => console.log('Redis Client Error', err));

client.connect()
  .then(() => console.log('Connected to Redis'))
  .catch(err => console.log('Failed to connect to Redis', err));

app.get('/', (req, res) => {
  res.send('Hello World!')
})

app.post('/', (req, res) => {
    let Smart_data = new Incomming_Data();
    Smart_data.Timestamp_recieved = Date.now();

    req.on('data', (chunk) => {
        Smart_data.Data_raw += chunk;
    });

    req.on('end', async () => {
        await Process_incomming_message(Smart_data);

        res.send(Smart_data.Data_raw);
    });
})

function Set_device_properties(data, element) {
    let Smart_data = data;

    if (element.Object == 'Device' && element.Resource == 'Get_identifier') {
        Smart_data.Identifier = element.Value;
        try {
            let Id_string = (element.Value + "   ").substring(0, 26);
            console.log(new Date(Date.now()).toLocaleString("en-GB"), ":", Id_string, "Objects:", Smart_data.Data_parsed.Data.length);
        }
        catch {
            // no code here
        }
    }

    if (element.Object == 'MCU'   && element.Resource == 'Get_uptime')         Smart_data.Device.Current_uptime = element.Value;
    if (element.Object == 'Modem' && element.Resource == 'Get_IP_address')     Smart_data.Device.IPv4 = element.Value;
    if (element.Object == 'Modem' && element.Resource == 'Get_mac_address')    Smart_data.Device.MAC = element.Value;
    if (element.Object == 'Modem' && element.Resource == 'Get_listen_port')    Smart_data.Device.Listen_port = element.Value;
    if (element.Object == 'Modem' && element.Resource == 'Get_server_address') Smart_data.Device.Send_server = element.Value;

    return Smart_data;
}

async function Process_incomming_message(data) {
    try {
        parseMessage(data);

        switch (data.Data_parsed.Protocol) {
            case Protocols.IPSO_v1:
                await handleIPSO(data);
                break;

            default:
                console.warn("Unknown protocol:", data.Data_parsed.Protocol);
        }

        await Handle_sensor_data(data);

    } catch (err) {
        console.error("Process_incomming_message failed:", err, data);
    }
}

function parseMessage(data) {
    if (!data.Data_raw || typeof data.Data_raw !== "string") {
        throw new Error("Data_raw missing or invalid");
    }

    if (!data.Data_parsed || Object.keys(data.Data_parsed).length === 0) {
        data.Data_parsed = JSON.parse(data.Data_raw);
    }

    if (!data.Data_parsed.Protocol) {
        throw new Error("Protocol missing");
    }
}

async function handleIPSO(data) {
    data.Protocol = Protocols.IPSO_v1;

    for (const element of data.Data_parsed.Data ?? []) {
        Set_device_properties(data, element);

        if (isSerialRX(element)) {
            await handleSerialMessage(data, element);
        }
    }
}

async function handleSerialMessage(parentData, element) {
    const child = new Incomming_Data();
    child.Timestamp_recieved = Date.now();
    child.Data_raw = element.Value;
    child.Identifier = parentData.Identifier;

    try {
        if (typeof element == "object") {
            child.Data_parsed = element.Value;
            child.Data_raw = JSON.stringify(element.Value);
        }
        else 
        {
            child.Data_parsed = JSON.parse(element.Value);
            child.Data_raw = element.Value;
        }
        
        await Process_incomming_message(child);
        await Set_gateway_info(parentData, child, element);

    } catch {
        await Handle_logging_data(child);
    }
}

function isSerialRX(element) {
    return (
        element.Object === "Serial" &&
        element.Resource === "Set_RX_message"
    );
}


app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})

function redisValue(value) {
    if (value === undefined || value === null) return "";
    if (typeof value === "object") return JSON.stringify(value);
    return value;
}

async function Set_gateway_info(Gateway, Client, Element) {
    // currently only serial devices behind gateways
    if (Element.Object !== "Serial" || Element.Resource !== "Set_RX_message") {
        return;
    }

    const deviceKey = `Device:${Client.Identifier}`;

    const baseFields = {
        Protocol:       Protocols.IPSO_v1,
        Identifier:     Client.Identifier,
        Last_activity:  Date.now(),
        First_activity: Date.now()
    };

    const gatewayFields = {
        Gateway_id:       Gateway.Identifier,
        Gateway_object:   Element.Object,
        Gateway_resource: Element.Resource,
        Gateway_instance: Element.Instance
    };

    const exists = await client.exists(deviceKey);

    await client.hSet(deviceKey, exists ? gatewayFields : baseFields);
}

async function Handle_sensor_data(Incomming_data = new Incomming_Data()) {
  const deviceKey   = `Device:${Incomming_data.Identifier}`;
  const messagesKey = `${deviceKey}:Messages`;
  const ioKey       = `${deviceKey}:IO_states`;
  const timerKey    = `${deviceKey}:Timers`;
  const exists      = await client.exists(deviceKey);

    if (!exists) {
        await client.hSet(deviceKey, {
        Protocol:       Protocols.IPSO_v1,
        Identifier:     Incomming_data.Identifier,
        Last_activity:  Date.now(),
        First_activity: Date.now()
        });
    } else {
        if (Incomming_data.Device.IPv4 != undefined) await client.hSet(deviceKey, 'IPv4', redisValue(Incomming_data.Device.IPv4));
        if (Incomming_data.Device.MAC)               await client.hSet(deviceKey, 'MAC', redisValue(Incomming_data.Device.MAC));
        if (Incomming_data.Device.Listen_port)       await client.hSet(deviceKey, 'Listen_port', redisValue(Incomming_data.Device.Listen_port));
        if (Incomming_data.Device.Send_server)       await client.hSet(deviceKey, 'Send_server', redisValue(Incomming_data.Device.Send_server));
        if (Incomming_data.Device.Current_uptime)    await client.hSet(deviceKey, 'Uptime', redisValue(Incomming_data.Device.Current_uptime));
                                                     await client.hSet(deviceKey, 'Last_activity', Date.now());
                                                     await client.hSet(deviceKey, 'Last_activity_hr', new Date().toISOString());

        if (Incomming_data.Data_parsed.Data != undefined) {
            Incomming_data.Data_parsed.Data.forEach(async element => {
                if (element.Object == "Digital" && element.Resource == "Get_io_enabled") {
                    await client.hSet(ioKey, "Digital_" + element.Instance, redisValue(element.Value));
                }

                if (element.Object == "Analog" && element.Resource == "Get_ADC") {
                    await client.hSet(ioKey, "Analog_" + element.Instance, redisValue(element.Value));
                }

                if (element.Object == "Timer") {
                    if (element.Resource == "Get_io_update")      await client.hSet(timerKey, "IO_update_" + element.Instance, redisValue(element.Value));
                    if (element.Resource == "Get_message_update") await client.hSet(timerKey, "Message_update_" + element.Instance, redisValue(element.Value));
                }
            })
        }
    }

  await client.rPush(messagesKey, redisValue(Incomming_data.Data_raw));
}

async function Handle_logging_data(Incomming_data = new Incomming_Data()) {
    const deviceKey = `Device:${Incomming_data.Identifier}`;
    const loggingKey = `${deviceKey}:Device_logging`;
    const exists = await client.exists(deviceKey);

    if (!exists) {
        await client.hSet(deviceKey, {
            Protocol: Protocols.IPSO_v1,
            Identifier: Incomming_data.Identifier,
            Last_activity: Date.now(),
            First_activity: Date.now()
        });
    } else {
        await client.hSet(deviceKey, 'Last_activity', Date.now());
        await client.hSet(deviceKey, 'Last_activity_hr', new Date().toISOString());
    }

    await client.rPush(loggingKey, redisValue(Incomming_data.Data_raw));
}

class Incomming_Data {
    Identifier = undefined;
    Data_raw = "";
    Protocol = Protocols.Unknown;
    Data_parsed = {}
    Timestamp_recieved = undefined;

    Raw_database = new Raw_Database();

    Device = new Device();
}

class Raw_Database {
    Is_saved = false;
    Must_be_saved = false;
    Row_number = undefined;
}

const Protocols = {
    Unknown: "Unknown",
    IPSO_v1: "IPSO_v1",
}

class Device {
    Identifier = undefined;
    MAC = undefined;
    IPv4 = undefined;
    Listen_port = undefined;
    Send_server = undefined;
    Data_protocol = Protocols.Unknown;
    Current_uptime = undefined;
    Stored_data = [];

    Currently_online = false;
    Last_activity = undefined;
    First_activity = undefined;
    Retiered = false;

    constructor(Identifier = undefined) {
        this.Identifier = Identifier;
    }
}