Templeton
=========

Templeton is a webapi on top of HCatalog and other Hadoop services.

Install
=======

Templeton is deep in the middle of development and doesn't have a
smooth install procedure.  As such, it should only be installed expert
developers.

0.  Requirements
  - ant >= 1.8
  - tomcat >= 7.0
  - Hadoop == 0.20.203.0 
  - hcat

1.  Setup these environment variables:

    | Var            | Value                                                   |
    |----------------+---------------------------------------------------------|
    | HADOOP_HOME    | Set to your Hadoop install                              |
    | CATALINA_HOME  | Set to your Tomcat install                              |
    | TEMPLETON_HOME | Set to the Templeton directory                          |
    | TEMPLETON_JAR  | The location of the Templeton jar file.                 |
    |                | After a build  the location will be                     |
    |                | $TEMPLETON_HOME/build/templeton/templeton-0.1.0-dev.jar |

2.  Start tomcat, making sure that the above environment vars are set
    for that process.

3.  Build and install the Templeton war file using "ant install-war"
    from $TEMPLETON_HOME.  The Tomcat webapps dir must be writable.

4.  Check that your local install works.  Assuming that tomcat is
    running on port 8080 on the local box, use:

        curl -i http://localhost:8080/templeton/v1/status.json

    A typical output is:

        HTTP/1.1 200 OK
        Server: Apache-Coyote/1.1
        Content-Type: application/json
        Transfer-Encoding: chunked
        Date: Tue, 18 Oct 2011 19:35:17 GMT

        {"status": "ok", "version": "v1"}

5.  To run any of the authenticated api calls, sudo must be setup
    properly.  For development, you can follow these simple steps:

    - Run tomcat as yourself.   
    - Make sure that you can sudo as yourself.  For example, 
        sudo -u $USER date

6.  To run the dll api call hcat must be installed in
    "/usr/local/hcat/bin/hcat". (!)

