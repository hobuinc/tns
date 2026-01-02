ARG LAMBDA_IMAGE="amazon/aws-lambda-provided:al2023-x86_64"
ARG RIE_ARCH="amd64"
ARG PYTHON_VERSION="3.13"

FROM --platform=$TARGETPLATFORM condaforge/miniforge3:latest as condasetup
LABEL MAINTAINER="Howard Butler <howard@hobu.co>"

ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETVARIANT
RUN printf "I'm building for TARGETPLATFORM=${TARGETPLATFORM}"
RUN printf ", TARGETARCH=${TARGETARCH}"
RUN printf ", TARGETVARIANT=${TARGETVARIANT} \n"
RUN printf "With uname -s : " && uname -s
RUN printf "and  uname -m : " && uname -mm

ENV CONDA_ENV_NAME "tns-lambda"
ENV CONDAENV "/opt/conda/envs/${CONDA_ENV_NAME}"


# Create the environment:
COPY build-environment.yml .
RUN mamba env create -f build-environment.yml --yes
# RUN mamba update --all -y

COPY run-environment.yml .
RUN cat run-environment.yml
RUN mamba env create -f run-environment.yml --yes


SHELL ["conda", "run", "-n", "build", "/bin/bash", "-c"]
RUN conda-pack -n ${CONDA_ENV_NAME} --dest-prefix=/var/task -o /tmp/env.tar
RUN mkdir /venv
RUN cd /venv && tar xf /tmp/env.tar  && rm /tmp/env.tar


FROM --platform=$TARGETPLATFORM ${LAMBDA_IMAGE:?} as al2

ARG RIE_ARCH
ARG LAMBDA_IMAGE
ARG TARGETPLATFORM
ARG TARGETARCH
ENV TARGETPLATFORM=${TARGETPLATFORM:-linux/amd64}
ENV TARGETARCH=${TARGETARCH:-amd64}



ENV CONDAENV "/var/task"
ENV CONDA_PREFIX "/var/task"
ENV TARGETPLATFORM "${TARGETPLATFORM}"
COPY --from=condasetup /venv ${CONDAENV}



ENV PROJ_LIB ${CONDAENV}/share/proj
ENV PROJ_NETWORK=TRUE
ENV PATH $PATH:${CONDAENV}/bin
ENV LD_LIBRARY_PATH=${CONDAENV}/lib
ENV HOME=/var/task/

RUN /var/task/bin/python -m pip install awslambdaric
ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie-${RIE_ARCH} /usr/bin/aws-lambda-rie

RUN chmod +x /usr/bin/aws-lambda-rie


WORKDIR /var/task
COPY python-entry.sh ./
RUN chmod +x /var/task/python-entry.sh
COPY handlers/ /var/task/lib/python${PYTHON_VERSION}/site-packages/tns_lambda
COPY handlers/config.py /var/task/
COPY root-bashrc /root/.bashrc
ENTRYPOINT [ "/var/task/python-entry.sh" ]