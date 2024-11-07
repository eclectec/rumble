Rumble is a simple flink analytic that pulls data from a kafka topic and populates a rumble topic with geojson objects for application consumption

## Build
Export a jar with your favorite IDE. Name jar rumble.jar and put in directory to share a volume with docker containers

## Run
Clone and build cloud and puddle docker containers

Navigate to software directory of choice

```
git clone git@github.com:eclectec/clouds.git
cd clouds
docker build -t cloud .
cd ../
git clone git@github.com:eclectec/puddles.git
cd puddles
docker build -t puddle .
```
In rumble project
```
docker compose up -d
```

To view output of analytic
```
docker logs puddle
```

To view flink logs and running jobs, navigate to http://localhost:8081