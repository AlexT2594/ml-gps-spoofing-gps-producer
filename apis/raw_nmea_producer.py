import falcon
import json

from utils.producer_consumer import publish_message, connect_kafka_producer

class RawNmeaProducerAPI:

    entry = set()
    previous_sent_entry = ""
    def on_post(self, req, res):
        """
        Expects json as:
        {
            "data": {
                "phrase": "$GPRMC,110029.00,A,4155.817848,N,01236.701207,E,000.0,,220219,,,A*7B"
            }
        }
        """


        req_data = json.loads(req.stream.read())
        phrase = req_data['data']['phrase']


        fields = phrase.split(',')
        message_ID = fields[0]

        phrase = phrase.rstrip()

        result = {
            "data": {
                "message": "Phrase received",
                "phrase": phrase
            }
        }

        res.content_type = falcon.MEDIA_JSON
        res.status = falcon.HTTP_200

        if message_ID == "$PGLOR":
            res.media = result
            return
        elif message_ID == "$GPRMC":
            self.entry.add(phrase)
            self.entry = list(self.entry)
            self.entry.sort(key=lambda phrase: phrase[3:6])
            result_entry = ";".join(self.entry)

            #if the entry doesn't contain at least the phrase $GPGGA we don't
            #consider, not even not stable ($GPGGA with QI=0)
            if result_entry[3:6] != "GGA":
                self.entry = set()
                return

            #if the current entry has the same timestamp of the last sent entry, we ignore it
            if self.previous_sent_entry != "" and self.previous_sent_entry[7:16] == result_entry[7:16]:
                self.entry = set()
                return

            print(result_entry)
            kafka_producer = connect_kafka_producer()
            publish_message(kafka_producer, 'raw_nmea', 'raw_nmea_message', result_entry)

            self.previous_sent_entry = result_entry
            if kafka_producer is not None:
                kafka_producer.close()

            self.entry = set()
        else:
            self.entry.add(phrase)
            result_entry = ";".join(self.entry)

        result["data"]["entry"] = result_entry
        res.media = result

