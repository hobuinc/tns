ARG LAMBDA_IMAGE="amazon/aws-lambda-provided:al2023-x86_64"
ARG RIE_ARCH="x86_64"
ARG PYTHON_VERSION="3.13"
ARG AWS_LAMBDA_RIE_VERSION="1.27"

FROM condaforge/miniforge3:latest AS condasetup
LABEL MAINTAINER="Howard Butler <howard@hobu.co>"

ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETVARIANT
RUN printf "I'm building for TARGETPLATFORM=${TARGETPLATFORM}"
RUN printf ", TARGETARCH=${TARGETARCH}"
RUN printf ", TARGETVARIANT=${TARGETVARIANT} \n"
RUN printf "With uname -s : " && uname -s
RUN printf "and  uname -m : " && uname -mm

ENV CONDA_ENV_NAME=tns-lambda
ENV CONDAENV=/var/task


# Create the environment:
COPY build-environment.yml .
RUN mamba env create -f build-environment.yml --yes

COPY run-environment.yml .
RUN mamba create --yes --prefix "${CONDAENV}" "python=${PYTHON_VERSION}" && \
    mamba env update --prefix "${CONDAENV}" --file run-environment.yml && \
    conda clean --all --yes


FROM ${LAMBDA_IMAGE:?} AS al2

ARG RIE_ARCH
ARG LAMBDA_IMAGE
ARG PYTHON_VERSION
ARG TARGETPLATFORM
ARG TARGETARCH
ARG AWS_LAMBDA_RIE_VERSION
ENV TARGETPLATFORM=${TARGETPLATFORM:-linux/amd64}
ENV TARGETARCH=${TARGETARCH:-amd64}
ENV CONDAENV=/var/task
ENV CONDA_PREFIX=/var/task
ENV PROJ_LIB=${CONDAENV}/share/proj
ENV PROJ_NETWORK=TRUE
ENV PATH=${PATH}:${CONDAENV}/bin
ENV LD_LIBRARY_PATH=${CONDAENV}/lib
ENV HOME=/var/task/

COPY --from=condasetup ${CONDAENV} ${CONDAENV}

RUN /var/task/bin/python -m pip install --no-cache-dir awslambdaric
ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/download/v${AWS_LAMBDA_RIE_VERSION}/aws-lambda-rie-${RIE_ARCH} /usr/bin/aws-lambda-rie

RUN chmod +x /usr/bin/aws-lambda-rie


WORKDIR /var/task
COPY python-entry.sh ./
RUN chmod +x /var/task/python-entry.sh
COPY handlers/ /var/task/lib/python${PYTHON_VERSION}/site-packages/tns_lambda/
COPY root-bashrc /root/.bashrc
ENTRYPOINT [ "/var/task/python-entry.sh" ]
