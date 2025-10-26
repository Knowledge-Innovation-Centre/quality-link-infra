FROM mageai/mageai:0.9.78

ARG BASE_CODE_PATH
ARG PROJECT_NAME

RUN mkdir -p ${BASE_CODE_PATH}/${PROJECT_NAME}

COPY requirements.txt ${BASE_CODE_PATH}/${PROJECT_NAME}/requirements.txt

RUN pip3 install -r ${BASE_CODE_PATH}/${PROJECT_NAME}/requirements.txt