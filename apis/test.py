import falcon

class TestAPI:
    def on_get(self, req, res):

        print(req)

        result = {
            "message": "OK"
        }

        res.media = result
        res.content_type = falcon.MEDIA_JSON
        res.status = falcon.HTTP_200