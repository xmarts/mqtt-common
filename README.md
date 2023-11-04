# Mqtt common library

### Usage

```py
from MqttLibPy.client import MqttClient
# from MqttLibPy.serializer import Serializer

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

#### Send file
Sender
```py
from MqttLibPy.client import MqttClient

client = MqttClient("myhost.com", 1883)
client.send_file("test_bytes", "/path/to/my/file/myfile.")
```

listener
```py
from MqttLibPy.client import MqttClient

client = MqttClient("myhost.com", 1883)

@client.endpoint("test_bytes", is_file=True)
def get_file(client, user_data, file):
    with open(f"/path/to/save/file/{file['data'][0]['filename']}", 'wb+') as f:
        f.write(file['bytes'])
        f.close()

threading.Thread(target=client.listen).start()
```


### Changelog

1.1.6
* Fix bug where the same file couldnt be received multiple times

1.1.0
* Add optional metadata to file transfers

1.0.3
* Fix bug where file metadata would get overwritten if metadata arrived after the file

1.0.2
* Fix bug that caused bytes to be empty if metadata arrived after the file

1.0.1
* Fix serializer crash with json objects

1.0.0
* Adds send file method