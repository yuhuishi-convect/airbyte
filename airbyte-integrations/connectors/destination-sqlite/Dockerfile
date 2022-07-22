FROM airbyte/integration-base-java:dev AS build

WORKDIR /airbyte
ENV APPLICATION destination-sqlite

COPY build/distributions/${APPLICATION}*.tar ${APPLICATION}.tar

RUN tar xf ${APPLICATION}.tar --strip-components=1 && rm -rf ${APPLICATION}.tar

FROM airbyte/integration-base-java:dev

WORKDIR /airbyte
ENV APPLICATION destination-sqlite

COPY --from=build /airbyte /airbyte

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/destination-sqlite
