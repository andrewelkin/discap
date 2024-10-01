
echo 'Storing 2 records:'

curl -X 'POST' \
  'http://localhost:8089?key=key1&value=value1&key=key2&value=value2' \
  -H 'accept: application/json' | jq

echo 'Check nodes len:'
curl -X 'GET' \
  'http://localhost:8089' \
  -H 'accept: application/json' | jq

echo 'Storing 2 more records:'
curl -X 'POST' \
  'http://localhost:8089?key=key3&value=value3&key=key4&value=value4' \
  -H 'accept: application/json' | jq

echo 'Check nodes len:'
curl -X 'GET' \
  'http://localhost:8089' \
  -H 'accept: application/json' | jq

echo 'Getting 4 keys:'
curl -X 'GET' \
  'http://localhost:8089?key=key1&key=key2&key=key3&key=key4' \
  -H 'accept: application/json' | jq

echo 'Getting legit key and abracadabra:'
curl -X 'GET' \
  'http://localhost:8089?key=key3&key=abra&key=cadabra' \
  -H 'accept: application/json' | jq

echo 'Deleting cache:'
curl -X 'DELETE' \
  'http://localhost:8089' \
  -H 'accept: application/json' | jq

echo 'Check nodes len:'
curl -X 'GET' \
  'http://localhost:8089' \
  -H 'accept: application/json' | jq


