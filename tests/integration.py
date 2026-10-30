import json
import os
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

binary = str(Path(sys.argv[1]).resolve())
requests = []
checks = 0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, status, body=None):
        self.send_response(status)
        self.end_headers()
        if body is not None:
            self.wfile.write(json.dumps(body).encode())

    def record(self):
        body = self.rfile.read(int(self.headers.get('Content-Length', 0))).decode()
        requests.append((self.command, self.path, dict(self.headers), body))
        return body

    def authenticated(self):
        if self.headers.get('Authorization') != 'Bearer test-token':
            self.respond(401)
            return False
        return True

    def do_POST(self):
        body = self.record()
        if self.path == '/realms/test/protocol/openid-connect/token':
            fields = parse_qs(body)
            if fields.get('password') != ['p&= +/ä']:
                self.respond(401)
                return
            if fields.get('client_id') != ['cli'] or fields.get('grant_type') != ['password']:
                self.respond(400)
                return
            self.respond(200, {'access_token': 'test-token', 'token_type': 'Bearer'})
        elif self.path == '/api/weave/api/v1/kafkatriggers' and self.authenticated():
            if self.headers.get('Content-Type') != 'application/json':
                self.respond(415)
                return
            payload = json.loads(body)
            if payload['name'] == 'conflict':
                self.respond(409, {'error': 'already exists'})
            else:
                self.respond(201, payload)

    def do_GET(self):
        self.record()
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')
            return
        if not self.authenticated():
            return
        parsed = urlsplit(self.path)
        query = parse_qs(parsed.query)
        if query.get('name') == ['fail']:
            self.respond(503, {'error': 'unavailable'})
        elif parsed.path == '/api/index/api/v1/artifacts':
            self.respond(200, {'query': query})
        else:
            self.respond(404)

    def do_DELETE(self):
        self.record()
        if not self.authenticated():
            return
        if self.path.endswith('/missing'):
            self.respond(404, {'error': 'not found'})
        else:
            self.respond(204)


server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
env = {key: value for key, value in os.environ.items() if not key.startswith('FUC_')}
url = f'http://127.0.0.1:{server.server_port}'
env.update(FUC_API_URL=url, FUC_KEYCLOAK_URL=url, FUC_REALM='test',
           FUC_CLIENT_ID='cli', FUC_USERNAME='user', FUC_PASSWORD='p&= +/ä')


def run(*args, success=True, overrides=None, network=None):
    global checks
    start = len(requests)
    result = subprocess.run([binary, *args], env=env | (overrides or {}),
                            cwd=directory, text=True, capture_output=True, timeout=10)
    assert (result.returncode == 0) == success, (args, result.stdout, result.stderr)
    if network is not None:
        assert len(requests) - start == network, (args, requests[start:])
    checks += 1
    return result.stdout if success else result.stderr


create = ['kafka', 'create', 'orders', '--chain', 'process-orders',
          '--broker', 'kafka:9092', '--topic', 'orders', '--group', 'workers']
try:
    with tempfile.TemporaryDirectory() as directory:
        assert 'kafka create' in run('--help', network=0)
        assert '--broker' in run('kafka', 'create', '--help', network=0)
        assert '--page-size' in run('artifacts', '--help', network=0)
        assert 'Test Keycloak' in run('login', '--help', network=0)
        assert run('health', network=1).strip() == 'OK'
        assert 'Authorization' not in requests[-1][2]
        assert 'succeeded' in run('login', network=1)
        assert '401' in run('login', success=False, overrides={'FUC_PASSWORD': 'bad'}, network=1)
        assert 'requires' in run('login', success=False, overrides={'FUC_PASSWORD': ''}, network=0)
        run('login', overrides={'FUC_CLIENT_SECRET': 's&=+'}, network=1)
        assert parse_qs(requests[-1][3])['client_secret'] == ['s&=+']
        payload = json.loads(run('artifacts', network=2))
        assert payload['query'] == {'page': ['0'], 'pageSize': ['20']}
        payload = json.loads(run('artifacts', '--page', '2', '--page-size=50',
                                 '--name', 'org & data', '--type', 'a', '--type', 'b',
                                 '--tag', 'latest', '--sort', 'name', '--order', 'asc',
                                 '--min-versions', '0', '--max-versions', '2', '--has-files', 'false', network=2))
        assert payload['query'] == {'page': ['2'], 'pageSize': ['50'], 'name': ['org & data'],
                                    'type': ['a', 'b'], 'tag': ['latest'], 'sort': ['name'],
                                    'order': ['asc'], 'minVersions': ['0'], 'maxVersions': ['2'], 'hasFiles': ['false']}
        payload = json.loads(run(*create, network=2))
        assert payload == {'name': 'orders', 'chainRef': {'name': 'process-orders'},
                           'kafka': {'brokers': ['kafka:9092'], 'topic': 'orders', 'consumerGroup': 'workers'}}
        payload = json.loads(run(*create, '--broker', 'kafka2:9092', '--secret', 'kafka-auth',
                                 '--event', 'put', '--event', 'delete', '--bucket', 'in"coming',
                                 '--bucket', 'other', '--max-concurrent', '0', network=2))
        assert payload['kafka'] == {'brokers': ['kafka:9092', 'kafka2:9092'], 'topic': 'orders',
                                    'consumerGroup': 'workers', 'secretRef': {'name': 'kafka-auth'},
                                    'eventFilter': ['put', 'delete'], 'bucketFilter': ['in"coming', 'other'],
                                    'maxConcurrentRuns': 0}
        assert requests[-1][2]['Content-Type'] == 'application/json'
        assert requests[-1][2]['Accept'] == 'application/json'
        assert requests[-1][2]['Authorization'] == 'Bearer test-token'
        preview = run(*create, '--dry-run', overrides={'FUC_CONFIG': '/does/not/exist'}, network=0)
        assert 'POST /api/weave/api/v1/kafkatriggers' in preview
        assert 'Content-Type: application/json' in preview
        assert 'test-token' not in preview and 'p&=' not in preview
        assert json.loads(preview.split('\n\n', 1)[1])['chainRef']['name'] == 'process-orders'
        assert '204' in run('kafka', 'delete', 'orders', network=2)
        assert requests[-1][0] == 'DELETE' and requests[-1][3] == ''
        run('kafka', 'delete', 'a/b?#', network=2)
        assert requests[-1][1].endswith('/a%2Fb%3F%23')
        assert 'Content-Type' not in requests[-1][2]
        assert 'DELETE' in run('kafka', 'delete', 'orders', '--dry-run', network=0)
        assert '404' in run('kafka', 'delete', 'missing', success=False, network=2)
        conflict = create.copy()
        conflict[2] = 'conflict'
        assert '409' in run(*conflict, success=False, network=2)
        assert '503' in run('artifacts', '--name', 'fail', success=False, network=2)
        assert '401' in run(*create, overrides={'FUC_PASSWORD': 'bad'}, success=False, network=1)
        for args in [
            ['list'], ['describe', 'GET /health'], ['call', 'GET /health'], ['artifact'],
            ['health', '--spec', 'openapi.yaml'], ['artifacts', '--query', 'page=0'],
            ['health', '--body', '{}'], ['health', '--header', 'X-Test=value'],
            ['health', '--no-auth'], ['login', '--dry-run'], ['kafka', 'update', 'orders'],
            ['kafka', 'delete'], ['kafka', 'delete', 'a', 'b'], ['kafka', 'delete', '..'],
            ['kafka', 'create', 'orders'], ['kafka', 'delete', 'orders', '--topic', 'x'],
            ['artifacts', '--page', '-1'], ['artifacts', '--page-size', '0'],
            ['artifacts', '--page', '1.5'], ['artifacts', '--page', '999999999999999999999999'],
            ['artifacts', '--page', '1', '--page', '2'], ['artifacts', '--sort', 'typo'],
            ['artifacts', '--has-files', 'yes'], ['artifacts', '--name='],
            ['artifacts', '--min-versions', '3', '--max-versions', '2'],
            ['artifacts', '--page', '--name', 'x'], ['artifacts', '--page'],
            create + ['--event', 'unknown'], create + ['--max-concurrent', '-1'],
            create + ['--topic', 'another'], create + ['--broker', ''],
            create + ['--dry-run', '--dry-run'],
        ]:
            run(*args, success=False, network=0)
        config = Path(directory) / 'config.yaml'
        config.write_text('api_url: http://127.0.0.1:1\nrealm: wrong\npassword: wrong\n')
        run('--config', str(config), 'artifacts', network=2)
        run('artifacts', '--config=' + str(config), network=2)
        run('artifacts', overrides={'FUC_CONFIG': str(config)}, network=2)
        run('--config', str(config), 'health', '--config', str(config), success=False, network=0)
        run('health', overrides={'FUC_TIMEOUT_SECONDS': '0'}, success=False, network=0)
        assert 'transport failed' in run('health', overrides={'FUC_API_URL': 'http://127.0.0.1:1'}, success=False, network=0)
        print(f'{checks} integration checks passed')
finally:
    server.shutdown()
    server.server_close()
