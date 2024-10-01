curl -X 'POST' \
  'http://localhost:8089?key=key1&value=value1&key=key2&value=value2' \
  -H 'accept: application/json' | jq

curl -X 'GET' \
  'http://localhost:8089' \
  -H 'accept: application/json' | jq

curl -X 'POST' \
  'http://localhost:8089?key=key3&value=value3&key=key4&value=value4' \
  -H 'accept: application/json' | jq

curl -X 'GET' \
  'http://localhost:8089?key=key1&key=key2&key=key3&key=key4' \
  -H 'accept: application/json' | jq


curl -X 'GET' \
  'http://localhost:8089?key=key3&key=abra&key=cadabra' \
  -H 'accept: application/json' | jq

curl -X 'DELETE' \
  'http://localhost:8089' \
  -H 'accept: application/json' | jq


curl -X 'GET' \
  'http://localhost:8089' \
  -H 'accept: application/json' | jq


