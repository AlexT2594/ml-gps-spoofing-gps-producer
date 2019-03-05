import falcon
from falcon_cors import CORS
from apis.raw_nmea_producer import RawNmeaProducerAPI

api = falcon.API()

# enable cors
cors = CORS(allow_origins_list=['*'])
public_cors = CORS(allow_all_origins=True,
                   allow_methods_list=['GET', 'POST'],
                   allow_headers_list=["content-type"])
api = falcon.API(middleware=[public_cors.middleware])

# describe apis here

api.add_route('/raw_nmea_producer/produce_data', RawNmeaProducerAPI())