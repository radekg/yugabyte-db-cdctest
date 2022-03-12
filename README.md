# YugabyteDB CDC client demo

A very simple YugabyteDB CDC client built for the blog post [YugabyteDB change data capture](https://gruchalski.com/posts/2022-02-23-yugabytedb-change-data-capture/) blog post. Read it to find out more.

## Build

```sh
docker build -t local/yugabyte-db-cdctest:latest .
```

## Run

### YugabyteDB prior to 2.13

This is the old style CDC approach:

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

Eventually, run directly from sources:

```sh
go run . \
  --database=cdctest \
  --table=region \
  --masters=127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102
```

### YugabyteDB 2.13+ CDC SDK

```sh
go run . \
  --database=cdctest \
  --mode=cdcsdk \
  --masters=127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102
```
