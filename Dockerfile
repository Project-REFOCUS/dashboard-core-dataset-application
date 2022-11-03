FROM python:3.10.8

COPY . /home

RUN pip install -r /home/requirements.txt

WORKDIR /home

CMD ["/usr/local/bin/python", "-m", "application.main"]
