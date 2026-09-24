# Variante 1

<project>
  <parent>
    <groupId>com.example</groupId>
    <artifactId>spark-parent</artifactId>
    <version>1.0.0</version>
  </parent>
  <artifactId>spark-local-bundle</artifactId>
  <packaging>jar</packaging>

  <build>
    <plugins>
      <plugin>
        <artifactId>maven-assembly-plugin</artifactId>
        <configuration>
          <descriptors>
            <descriptor>src/assembly/bundle.xml</descriptor>
          </descriptors>
        </configuration>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>single</goal></goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>

mit 

<assembly>
  <id>bundle</id>
  <formats><format>jar</format></formats>
  <includeBaseDirectory>false</includeBaseDirectory>
  <containerDescriptorHandlers>
    <containerDescriptorHandler>
      <handlerName>metaInf-services</handlerName>
    </containerDescriptorHandler>
  </containerDescriptorHandlers>
  <dependencySets>
    <dependencySet>
      <scope>provided</scope>
      <unpack>true</unpack>
      <useProjectArtifact>false</useProjectArtifact>
      <excludes>
        <exclude>org.apache.spark:*</exclude>
        <exclude>org.scala-lang:*</exclude>
      </excludes>
    </dependencySet>
  </dependencySets>
</assembly>

# Variante 2

mvn dependency:copy \
  -Dartifact=com.example:spark-parent:1.0.0:pom \
  -DoutputDirectory=/tmp/spark-parent

mvn -f /tmp/spark-parent/spark-parent-1.0.0.pom \
  dependency:copy-dependencies \
  -DincludeScope=provided \
  -DexcludeGroupIds=org.apache.spark,org.scala-lang \
  -DoutputDirectory=$HOME/spark-local-jars
