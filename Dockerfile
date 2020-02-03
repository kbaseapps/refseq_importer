FROM kbase/sdkbase2:python
MAINTAINER KBase Developer
# -----------------------------------------
# In this section, you can install any system dependencies required
# to run your App.  For instance, you could place an apt-get update or
# install line here, a git checkout to download code, or run any other
# installation scripts.

# RUN apt-get update


# -----------------------------------------

COPY ./ /kb/module
RUN mkdir -p /kb/module/work && mkdir /data
RUN chmod -R a+rw /kb/module && chmod -R a+rw /data

WORKDIR /kb/module

RUN make all

RUN pip install --upgrade pip plyvel

ENTRYPOINT [ "./scripts/entrypoint.sh" ]

ENV PYTHONUNBUFFERED=true

CMD [ ]
