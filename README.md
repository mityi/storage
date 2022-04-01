# storage

```shell
#build
docker build -t mityi/storage:-0.0.0-SNAPSHOT .
#local
docker run -it --rm -p 3000:3000 mityi/storage:-0.0.0-SNAPSHOT 
#push
docker push mityi/storage:-0.0.0-SNAPSHOT
#
#docker run -d --name storage --log-opt max-size=10m --log-opt max-file=5 --restart always -p 127.0.0.1:3777:3777 mityi/storage:0.0.0-SNAPSHOT
```
