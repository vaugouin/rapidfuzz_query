docker build -t rapidfuzz_query-python-app .
docker run -it --rm --network="host" --env-file /home/debian/docker/rapidfuzz_query/.env --name rapidfuzz_query rapidfuzz_query-python-app
