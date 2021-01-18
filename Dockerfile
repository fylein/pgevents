FROM python:3.8-slim

#################################################
# Install requirements
#################################################

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

#################################################
# Copy over code
#################################################

RUN mkdir -p /fyle_pgevents

WORKDIR /fyle_pgevents

COPY . /fyle_pgevents/

RUN pylint --rcfile=.pylintrc common *.py

RUN pip install .

ENV PYTHONUNBUFFERED 1
CMD ["/bin/bash"]
