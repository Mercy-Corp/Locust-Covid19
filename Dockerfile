FROM python:3.7

# set a directory for the app

WORKDIR /usr/src/locustcovid19

# copy all the files to the container
COPY . .

ADD https://bootstrap.pypa.io/get-pip.py .
RUN python3 get-pip.py --user && \
pip3 install -r requirements.txt

ENTRYPOINT ["python3", "locustcovid19/locustcovid19"]
