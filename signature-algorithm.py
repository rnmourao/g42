import sys
import hashlib
import hmac
import binascii
from datetime import datetime


yourSecretAccessKeyID = '275hSvB6EEOorBNsMDEfOaICQnilYaPZhXUaSK64'
httpMethod = "PUT"
contentType = "application/xml"

# "date" is the time when the request was actually generated
date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
canonicalizedHeaders = "x-obs-acl:private"
CanonicalizedResource = "/newbucketname2"
canonical_string = httpMethod + "\n" + "\n" + contentType + "\n" + date + "\n" + canonicalizedHeaders + CanonicalizedResource

hashed = hmac.new(yourSecretAccessKeyID.encode('UTF-8'), canonical_string.encode('UTF-8'),hashlib.sha1)    
encode_canonical = binascii.b2a_base64(hashed.digest())[:-1].decode('UTF-8')

print encode_canonical
