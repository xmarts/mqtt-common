# Mqtt common library

### Usage

```py
from MqttLibPy import MqttClient, serializer

# Both prefix and postfix are optional
client = MqttClient('myhost.com', 1883, prefix="myprefix")

# This function will be  called when a message is received in the myprefix/myendpoint topic  
@client.endpoint("myendpoint", force_json=True)
def myendpoint(mqtt_client, _, json_body):
    print(json_body)
    my_field = json_body["some_field"]
    # Do stuff with my_field
    # ..
    # Return a response
    my_response = {"another_field": "Ok!"}
    # Sends message to topic "myendpoint"
    client.send_message_serialized(my_response, "myendpoint", valid_json=True)
```