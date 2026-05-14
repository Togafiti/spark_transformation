FROM starburstdata/hive:3.1.2-e.18

USER root
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar /opt/apache-hive-3.1.2-bin/lib/aws-java-sdk-bundle-1.11.901.jar
RUN chmod 644 /opt/apache-hive-3.1.2-bin/lib/aws-java-sdk-bundle-1.11.901.jar
USER hive
