# YugabyteDB CDC client demo

A very simple YugabyteDB CDC client built for the blog post published at https://gruchalski.com. Read the article to find out more.

## Build

```sh
docker build -t local/yugabyte-db-cdctest:latest .
```

## Run

```sh
docker run --net=yb-dbnet --rm \
  -ti local/yugabyte-db-cdctest:latest \
  --database=cdctest \
  --table=region \
  --masters=yb-master-n1:7100,yb-master-n2:7100,yb-master-n3:7100
```  

If you know the stream ID:

```sh
docker run --net=yb-dbnet --rm \
  -ti local/yugabyte-db-cdctest:latest \
  --database=cdctest \
  --table=region \
  --masters=yb-master-n1:7100,yb-master-n2:7100,yb-master-n3:7100 \
  --stream-id=2d960eb58b144f12944f1350990244cd
```
