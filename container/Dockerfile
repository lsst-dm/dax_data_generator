# Using the Qserv development toolchain
FROM qserv/qserv:deps_20200729_0234

USER 1000

RUN source /qserv/stack/loadLSST.bash && \
    setup -t qserv-dev qserv_distrib && \
    git clone https://github.com/lsst-dm/dax_data_generator.git && \
    cd dax_data_generator && \
    git checkout tickets/test && \
    python setup.py develop
