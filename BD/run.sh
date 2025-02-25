#!/bin/bash
echo "Run docker-compose..."
docker-compose up -d --build

is_running() {
  container_name="$1"
  status=$(docker inspect -f '{{.State.Running}}' "$container_name" 2>/dev/null || echo "false")
  echo "$status"
}

echo "Waiting for the producer and consumer..."

while true; do
  prod_status=$(is_running producer)
  cons_status=$(is_running consumer)
  k1_status=$(is_running kafka1)
  k2_status=$(is_running kafka2)

  echo $(docker inspect -f '{{.State.Running}}' bd-consumer-1)

  if [ $(docker inspect -f '{{.State.Running}}' bd-producer-1) != "true" ] && [ $(docker inspect -f '{{.State.Running}}' bd-consumer-1) != "true" ] && [ $(docker inspect -f '{{.State.Running}}' bd-kafka1-1) == "true" ] && [ $(docker inspect -f '{{.State.Running}}' bd-kafka2-1) == "true" ]; then
    echo "Producer and consumer were finished"
    break
  fi

  sleep 5
done

echo "Stop docker-compose..."
docker-compose down

echo "All services were stoped"
