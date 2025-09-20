import boto3
from botocore import awsrequest
from botocore import crt
import urllib.parse

failover_header = 'originTypeFailover'
cf_read_only_headers_list = [h.lower() for h in [
    'Accept-Encoding',
    'Content-Length',
    'If-Modified-Since',
    'If-None-Match',
    'If-Range',
    'If-Unmodified-Since',
    'Transfer-Encoding',
    'Via'
]]


class SigV4AWrapper:

    def __init__(self):
        self._session = boto3.Session()

    def get_auth_headers(self, method, endpoint, data, region, service, headers, params=None):
        sigv4a = crt.auth.CrtS3SigV4AsymAuth(self._session.get_credentials(), service, region)
        request = awsrequest.AWSRequest(method=method, url=endpoint, data=data, headers=headers, params=params)
        sigv4a.add_auth(request)
        prepped = request.prepare()
        return prepped.headers


def handler(event, context):
    request = event['Records'][0]['cf']['request']

    origin_key = list(request['origin'].keys())[0]
    custom_headers = request['origin'][origin_key].get('customHeaders', {})

    # Check failover case. If CloudFront origin customer header is included that signals it's the failover request.
    # In this case, assumed, SigV4A singing should not be performed and
    # unmodified request should be used for the failover origin.
    if failover_header in custom_headers:
        return request

    method = request["method"]
    uri = request['uri']
    domain_name = request['origin']['custom']['domainName']
    
    # Ensure the URI path is properly encoded for S3
    # CloudFront may pass decoded URIs, but S3 expects proper encoding
    # Parse the URI to ensure consistent encoding
    parsed_uri = urllib.parse.urlparse(uri)
    # Re-encode the path to ensure proper formatting
    encoded_path = urllib.parse.quote(parsed_uri.path, safe='/')
    
    # Create the endpoint URL with properly encoded path
    endpoint = f"https://{domain_name}{encoded_path}"
    
    data = None  # Empty for GET requests
    region = '*'  # For S3 Multi-Region Access Point it's '*' (all regions)
    service = 's3'

    headers = request["headers"]
    request_headers_list = [h.lower() for h in headers.keys()]

    signing_headers = {}
    
    # Include CloudFront read-only headers that must be part of the signature
    for h in cf_read_only_headers_list:
        if h in request_headers_list:
            # Find the actual header key (case-sensitive)
            for header_key in headers.keys():
                if header_key.lower() == h:
                    signing_headers[headers[header_key][0]['key']] = headers[header_key][0]['value']
                    break

    # Add required headers for S3 signing
    signing_headers['Host'] = domain_name
    signing_headers['X-Amz-Cf-Id'] = event['Records'][0]['cf']['config']['requestId']

    # Handle query string parameters if they exist
    params = None
    querystring = request.get('querystring', '')
    if querystring:
        params = {}
        for param_pair in querystring.split('&'):
            if '=' in param_pair:
                key, value = param_pair.split('=', 1)
                key = urllib.parse.unquote_plus(key)
                value = urllib.parse.unquote_plus(value)
                params[key] = value
            else:
                key = urllib.parse.unquote_plus(param_pair)
                params[key] = ''

    # Sign the request with SigV4A
    auth_headers = SigV4AWrapper().get_auth_headers(method, endpoint, data, region, service, signing_headers, params)

    # Remove X-Amz-Cf-Id as CloudFront will add it automatically
    auth_headers.pop('X-Amz-Cf-Id', None)

    # Convert to CloudFront header format
    cf_headers = {}
    for k, v in auth_headers.items():
        cf_headers[k.lower()] = [{'key': k, 'value': v}]

    # Update request headers with signed headers
    request['headers'] = cf_headers

    return request