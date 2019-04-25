import falcon
import json
import jwt

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

        res.content_type = falcon.MEDIA_JSON
        res.status = falcon.HTTP_400

        jwt_token = req.get_header('Authorization')
        public_key = open('public_jwt_key.pem').read()
        try:
            payload = jwt.decode(jwt_token, public_key, algorithms=['RS256'])
            if payload['sub'] != 'gps_receiver':
                return
        except:
            return
        

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


        # we ignore not useful phrases
        if message_ID != "$GPGGA" and message_ID != "$GPGSA" and message_ID != "$GPGSV" and message_ID != "$GPRMC":
            res.media = result
            return
        elif message_ID == "$GPRMC":
            self.entry.add(phrase)
            self.entry = list(self.entry)
            self.entry.sort(key=lambda phrase: phrase[:10]) #[:10] because we want to sort also the GSV phrases
            
            #print("==> Entry array")
            #print(self.entry)

            #we need to check that all $GPGSV entries are present
            for entry_index in range(len(self.entry)):
                current_phrase = self.entry[entry_index]
                if current_phrase[:6] == "$GPGSV":
                    current_phrase_array = current_phrase.split(",")
                    total_GSV_messages = int(current_phrase_array[1])

                    for GSV_index in range(entry_index, entry_index + total_GSV_messages):
                        # if the GSV_index reaches the end then some phrases are missing
                        if GSV_index >= len(self.entry):
                            self.entry = set()
                            return
                        # if this phrase is not GSV then for sure some GSV message is missing
                        # order is verified by the sorting function
                        elif self.entry[GSV_index][:6] != "$GPGSV":
                            self.entry = set()
                            return

                    # if I reach this point then I don't need to do other checks
                    break
            
            # we need to remove some particular duplicates
            to_remove_indices = list()
            for entry_index in range(len(self.entry)):
                current_phrase = self.entry[entry_index]
                if current_phrase[:6] == "$GPGSV" and entry_index - 1 >= 0:
                    previous_phrase = self.entry[entry_index - 1]

                    if previous_phrase[:6] == "$GPGSV" and current_phrase[9:10] == previous_phrase[9:10]:
                        to_remove_indices.append(entry_index)

            for index in to_remove_indices:
                del self.entry[index]
            
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
        res.status = falcon.HTTP_200
        res.media = result

