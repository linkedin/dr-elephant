 ***
To enable interaction between Dr. Elephant and Azkaban using elephant headless account and it's private key:

I copied the AzkabanClient.java code from azcli project in Dr. Elephant (instead of adding it as a dependency) and
added it's dependency in Dr. Elephant.

(current AzkabanClient.java in this package is different, it was written using https://github.com/linkedin/linkedin-gradle-plugin-for-apache-hadoop/blob/master/hadoop-plugin/src/main/groovy/com/linkedin/gradle/azkaban/AzkabanUploadTask.groovy)

The dependencies causing error are:
"org.glassfish.jersey.core" % "jersey-client" % "2.8",
"org.glassfish.jersey.media" % "jersey-media-json-jettison" % "2.8" intransitive(),
"org.glassfish.jersey.media" % "jersey-media-multipart" % "2.8",

Error:
[warn]     ::          UNRESOLVED DEPENDENCIES
[warn]     :: org.glassfish.hk2#hk2-utils;2.8: not found
[warn]     :: org.glassfish.hk2#hk2-locator;2.8: not found
sbt.ResolveException: unresolved dependency: org.glassfish.hk2#hk2-utils;2.8: not found
unresolved dependency: org.glassfish.hk2#hk2-locator;2.8: not found


Fangshi suggested to add azcli as dependency rather than adding azcli's dependencies to Dr. Elephant. I am not sure how will be helpful. Please get in touch with him in case of any problem.

 ***


 ***
 Sample log in json format is there is in resource directory which can be emitted to kafka. Look at the header field for values.

 ***
