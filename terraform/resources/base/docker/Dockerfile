ARG LAMBDA_IMAGE="amazon/aws-lambda-provided:al2023-x86_64"
ARG RIE_ARCH="x86_64"
ARG PYTHON_VERSION="3.13"
ARG AWS_LAMBDA_RIE_VERSION="1.27"
ARG PIXI_VERSION="v0.65.0"

FROM amazonlinux:2023 AS pixisetup
LABEL MAINTAINER="Howard Butler <howard@hobu.co>"

ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETVARIANT
ARG PIXI_VERSION
RUN printf "I'm building for TARGETPLATFORM=${TARGETPLATFORM}"
RUN printf ", TARGETARCH=${TARGETARCH}"
RUN printf ", TARGETVARIANT=${TARGETVARIANT} \n"
RUN printf "With uname -s : " && uname -s
RUN printf "and  uname -m : " && uname -mm

ENV PIXI_HOME=/opt/pixi
ENV PATH=${PIXI_HOME}/bin:${PATH}
WORKDIR /tmp/workspace

RUN dnf install --assumeyes tar gzip bzip2 findutils shadow-utils && dnf clean all
RUN curl -fsSL https://pixi.sh/install.sh | PIXI_VERSION=${PIXI_VERSION} PIXI_HOME=${PIXI_HOME} bash

COPY pixi.toml pixi.lock /tmp/workspace/
RUN pixi install --manifest-path /tmp/workspace/pixi.toml --locked


FROM ${LAMBDA_IMAGE:?} AS al2

ARG RIE_ARCH
ARG LAMBDA_IMAGE
ARG PYTHON_VERSION
ARG TARGETPLATFORM
ARG TARGETARCH
ARG AWS_LAMBDA_RIE_VERSION
ENV TARGETPLATFORM=${TARGETPLATFORM:-linux/amd64}
ENV TARGETARCH=${TARGETARCH:-amd64}
ENV TNS_ENV=/var/task
ENV PROJ_LIB=${TNS_ENV}/share/proj
ENV PROJ_NETWORK=TRUE
ENV PATH=${PATH}:${TNS_ENV}/bin
ENV LD_LIBRARY_PATH=${TNS_ENV}/lib
ENV HOME=/var/task/

COPY --from=pixisetup /tmp/workspace/.pixi/envs/default ${TNS_ENV}
ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/download/v${AWS_LAMBDA_RIE_VERSION}/aws-lambda-rie-${RIE_ARCH} /usr/bin/aws-lambda-rie

RUN chmod +x /usr/bin/aws-lambda-rie


WORKDIR /var/task
COPY python-entry.sh ./
RUN chmod +x /var/task/python-entry.sh
COPY handlers/ /var/task/lib/python${PYTHON_VERSION}/site-packages/tns_lambda/
ENTRYPOINT [ "/var/task/python-entry.sh" ]
