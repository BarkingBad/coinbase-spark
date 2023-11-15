FROM ubuntu:latest

COPY Mock.scala /
COPY Mock.scala /home

RUN apt update -y && \
    apt upgrade -y && \
    apt install -y curl

RUN curl -sSLf https://scala-cli.virtuslab.org/get | sh

WORKDIR home

RUN \
  echo "println(1)" | /root/.cache/scalacli/local-repo/bin/scala-cli/scala-cli -S 3.0.2 - && \
  echo "//> using dep org.java-websocket:Java-WebSocket:1.5.2" | /root/.cache/scalacli/local-repo/bin/scala-cli/scala-cli -S 3.0.2  - && \
  echo "//> using dep org.slf4j:slf4j-simple:1.7.25" | /root/.cache/scalacli/local-repo/bin/scala-cli/scala-cli -S 3.0.2  -

ENTRYPOINT ["/root/.cache/scalacli/local-repo/bin/scala-cli/scala-cli"]
CMD ["-S", "3.0.2",  "Mock.scala"]
