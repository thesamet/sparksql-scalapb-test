# sparksql-scalapb-test

Test project for SparkSQL and ScalaPB.

To build and deploy:

    $ sbt assembly

Copy the input file input.base64.txt to /tmp/ directory on all workers.

Submit the job to your cluster:

    ./bin/spark-submit --master spark://10.1.2.3:7077 \
      --jars \
      --class RunDemo \
      /path/to/sparksql-scalapb-test-assembly-0.1-SNAPSHOT.jar
