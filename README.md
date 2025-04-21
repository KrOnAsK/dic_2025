## Running locally

```
python runner.py reviews_devset.json --stopwords stopwords.txt
```

## Running on cluster

```
python3 runner.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -r hadoop hdfs:///user/dic25_shared/amazon-reviews/full/reviews_devset.json --stopwords stopwords.tx
t
```

Get status (change id to the one logged in console):

```
curl http://lbdmg01.datalab.novalocal:8088/cluster/app/application_1744278675156_0846
```
