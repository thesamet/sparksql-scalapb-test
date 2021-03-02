# sparksql-scalapb-test

Test project for SparkSQL and ScalaPB.

1. To build:

    $ sbt assembly

Note a line like the follows that provides the path to the JAR we created:

    [info] Packaging /home/.../sparksql-scalapb-test/target/scala-2.12/sparksql-scalapb-test-assembly-0.1.0-SNAPSHOT.jar ...

2. Submit the job to your cluster:

    /path/to/spark/bin/spark-submit \
      --jars . \
      --class myexample.RunDemo \
      target/scala-2.12/sparksql-scalapb-test-assembly-0.1-SNAPSHOT.jar

