from mainLogic.utils.glv import Global


class Endpoint:
    def __init__(self,
                 url=None,
                 method='GET',
                 headers=None,
                 payload=None,
                 files=None,
                 post_function=None,
                 ):

        if files is None:
            files = {}
        if payload is None:
            payload = {}
        if headers is None:
            headers = {}

        self.url = url
        self.method = method
        self.headers = headers
        self.payload = payload
        self.files = files
        self.post_function = post_function

    def __str__(self):
        return f'Endpoint(url={self.url}, method={self.method}, headers={self.headers}, payload={self.payload}, files={self.files}, post_function={self.post_function})'

    def __repr__(self):
        return self.__str__()

    def __dict__(self):
        return {
            'url': self.url,
            'method': self.method,
            'headers': self.headers,
            'payload': self.payload,
            'files': self.files,
            'post_function': self.post_function
        }

    def __eq__(self, other):
        if not isinstance(other, Endpoint):
            return False
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.__str__())

    def __copy__(self):
        return Endpoint(
            url=self.url,
            method=self.method,
            headers=self.headers,
            payload=self.payload,
            files=self.files,
            post_function=self.post_function
        )

    def fetch(self):
        import requests
        response = requests.request(
            method=self.method,
            url=self.url,
            headers=self.headers,
            data=self.payload,
            files=self.files
        )

        # check if the response is valid and can be a json
        response_obj = None
        try:
            response_obj = response.json()
        except:
            response_obj = response.text
        finally:
            if self.post_function:
                if callable(self.post_function):
                    self.post_function(response_obj)
                else:
                    raise ValueError('post_function must be callable')

        # Optional verbose request/response logging for diagnostics
        try:
            import os, json, time
            if os.getenv('PWDL_VERBOSE_REQ'):
                safe_name = str(int(time.time()))
                fn = f"/tmp/endpoint_log_{safe_name}.json"
                dump = {
                    'request': {
                        'method': self.method,
                        'url': self.url,
                        'headers': self.headers,
                        'payload': self.payload,
                        'files': list(self.files.keys())
                    },
                    'response': {
                        'status_code': response.status_code,
                        'headers': dict(response.headers),
                        'body': None
                    }
                }
                try:
                    dump['response']['body'] = response_obj
                except Exception:
                    dump['response']['body'] = response.text

                try:
                    with open(fn, 'w') as fh:
                        fh.write(json.dumps(dump, default=str, indent=2))
                except Exception:
                    pass
        except Exception:
            pass

        return response_obj, response.status_code, response
