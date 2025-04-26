## Running locally

```
python main.py reviews_devset.json --stopwords stopwords.txt
```

## Running on cluster

```
python main.py hdfs:///user/dic25_shared/amazon-reviews/full/reviews_devset.json --stopwords stopwords.txt > output.txt
t
```

Get status (change id to the one logged in console):

```
curl http://lbdmg01.datalab.novalocal:8088/cluster/app/application_1744278675156_0846
```
