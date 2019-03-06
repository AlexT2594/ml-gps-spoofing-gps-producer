import falcon
import json

from utils.producer_consumer import publish_message, connect_kafka_producer

class RawNmeaProducerAPI:

    entry = ""
    def on_post(self, req, res):
        """
        Expects json as:
        {
            "data": {
                "type": "$GPRMC", 
                "phrase": "$GPRMC,110029.00,A,4155.817848,N,01236.701207,E,000.0,,220219,,,A*7B"
            }
        }
        """
        req_data = json.loads(req.stream.read())
        phrase = req_data['data']['phrase']


        fields = phrase.split(',')
        message_ID = fields[0]

        phrase = phrase.rstrip()

        if message_ID == "$GPRMC":
            self.entry += phrase
            
            kafka_producer = connect_kafka_producer()
            publish_message(kafka_producer, 'raw_nmea', 'raw_nmea_message', self.entry)

            if kafka_producer is not None:
                kafka_producer.close()

            result_entry = self.entry
            self.entry = ""
        else:
            self.entry += phrase + ";"
            result_entry = self.entry

        result = {
            "data": {
                "message": "Phrase received",
                "phrase": phrase,
                "entry": result_entry
            }
        }

        res.media = result
        res.content_type = falcon.MEDIA_JSON
        res.status = falcon.HTTP_200

