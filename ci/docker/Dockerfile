ARG BASE_VERSION

FROM adoptopenjdk/openjdk11:alpine
ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean jar \
    && ls -l build/libs

FROM emcmongoose/mongoose-base:${BASE_VERSION}
ARG BASE_VERSION
ARG STORAGE_DRIVER_COOP_VERSION
ARG VERSION
RUN mkdir -p $HOME/.mongoose/${BASE_VERSION}/ext
COPY --from=0 /build/libs/mongoose-storage-driver-aio-${VERSION}.jar /tmp/
ADD ci/docker/entrypoint_storage_driver_aio.sh /opt/mongoose/entrypoint_storage_driver_aio.sh
RUN mv -f /tmp/mongoose-storage-driver-aio-*.jar $HOME/.mongoose/${BASE_VERSION}/ext/ \
    && chmod +x /opt/mongoose/entrypoint_storage_driver_aio.sh \
    && curl http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-coop/${STORAGE_DRIVER_COOP_VERSION}/mongoose-storage-driver-coop-${STORAGE_DRIVER_COOP_VERSION}.jar -o $HOME/.mongoose/${BASE_VERSION}/ext/mongoose-storage-driver-coop.jar
ENTRYPOINT ["/opt/mongoose/entrypoint_storage_driver_aio.sh"]
